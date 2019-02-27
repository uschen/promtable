package promtable

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"cloud.google.com/go/bigtable"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/uschen/promtable/prompb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	defaultNamespace = "default"
)

// Server -
type Server struct {
	httpServer *http.Server

	cfg *Config

	store *Store

	mux *http.ServeMux

	l net.Listener

	Logger *zap.Logger
}

// NewServerWithConfig -
func NewServerWithConfig(cfg *Config) (*Server, error) {
	s := &Server{
		httpServer: &http.Server{},
		cfg:        cfg,
		mux:        http.NewServeMux(),
	}

	s.httpServer.Handler = s.mux

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	s.Logger = logger

	// bigtable client
	btcops := []option.ClientOption{}
	btcops = append(btcops, btcops...)
	if cfg.Bigtable.Instance == "" {
		return nil, errors.New("bigtable.instance is required")
	}
	if cfg.Bigtable.KeyPath != "" {
		btcops = append(btcops, option.WithServiceAccountFile(cfg.Bigtable.KeyPath))
	}
	btc, err := bigtable.NewClient(
		context.Background(),
		cfg.Bigtable.ProjectID,
		cfg.Bigtable.Instance,
		btcops...)
	if err != nil {
		return nil, err
	}

	var storeOps = []StoreOptionFunc{
		StoreWithBigtableClient(btc),
		StoreWithTableName(cfg.Bigtable.Table),
	}

	if cfg.EnsureTables {
		btac, err := bigtable.NewAdminClient(
			context.Background(),
			cfg.Bigtable.ProjectID,
			cfg.Bigtable.Instance,
			btcops...)
		if err != nil {
			return nil, err
		}
		storeOps = append(storeOps, StoreWithBigtableAdminClient(btac))
	}

	store, err := NewStore(storeOps...)
	if err != nil {
		return nil, err
	}

	s.store = store

	s.mux.Handle("/write", http.HandlerFunc(s.HandleWrite))
	s.mux.Handle("/read", http.HandlerFunc(s.HandleRead))

	lis, err := net.Listen("tcp", cfg.Web.Listen)
	if err != nil {
		return nil, err
	}
	s.l = lis

	return s, nil
}

// HandleWrite -
func (s *Server) HandleWrite(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		// log.Error("msg", "Read error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		// log.Error("msg", "Decode error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req = new(prompb.WriteRequest)
	if err := proto.Unmarshal(reqBuf, req); err != nil {
		// log.Error("msg", "Unmarshal error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.store.Put(r.Context(), defaultNamespace, req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleRead -
func (s *Server) HandleRead(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		// log.Error("msg", "Read error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		// log.Error("msg", "Decode error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req = new(prompb.ReadRequest)
	if err := proto.Unmarshal(reqBuf, req); err != nil {
		// log.Error("msg", "Unmarshal error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.store.Read(r.Context(), defaultNamespace, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Run -
func (s *Server) Run() error {
	s.Logger.Info("Prepare Server.Run()")
	var wg sync.WaitGroup
	wgCtx, wgCancel := context.WithCancel(context.Background())
	defer wgCancel()

	// main server, in case we have other servers in the future, like metrics...
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer wgCancel()
		if err := s.httpServer.Serve(s.l); err != nil {
			s.Logger.Error("http server exited with error", zap.Error(err))
		}
	}()
	s.Logger.Info("HTTP Server Start", zap.String("Listen", s.cfg.Web.Listen))

	// wait for signal
	stop := make(chan os.Signal)
	defer close(stop)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-wgCtx.Done():
		s.Logger.Info("some server returned, graceful shutdown all\n")
	case sig := <-stop:
		s.Logger.Warn("system signal received, will graceful shutdown", zap.String("signal", sig.String()))
	}

	if err := s.Stop(context.Background()); err != nil {
		s.Logger.Warn("close http server error", zap.Error(err))
	}
	// wait for them to stop
	wg.Wait()
	s.Logger.Info("Exit")
	return nil
}

// Stop -
func (s *Server) Stop(ctx context.Context) error {
	var errs []error

	if err := s.httpServer.Shutdown(ctx); err != nil && grpc.Code(err) != codes.Canceled {
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return fmt.Errorf("HTTPServer.Close with errors: %v", errs)
	}
	return nil
}
