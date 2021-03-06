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
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uschen/promtable/prompb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Server -
type Server struct {
	httpServer *http.Server

	cfg *Config

	store *Store

	mux *http.ServeMux

	l net.Listener

	// prometheus metric
	psrv      *http.Server
	plistener net.Listener

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

	getBTOpts := func(btcfg BT) ([]option.ClientOption, *bigtable.Client, error) {
		// bigtable client
		btcops := []option.ClientOption{}
		if btcfg.Instance == "" {
			return nil, nil, errors.New("bigtable.instance is required")
		}
		if btcfg.KeyPath != "" {
			btcops = append(btcops, option.WithServiceAccountFile(btcfg.KeyPath))
		}
		btc, err := bigtable.NewClient(
			context.Background(),
			btcfg.ProjectID,
			btcfg.Instance,
			btcops...)
		if err != nil {
			return nil, nil, err
		}
		return btcops, btc, nil
	}

	// normal bt
	btcops, btc, err := getBTOpts(cfg.Bigtable)
	if err != nil {
		return nil, err
	}

	var storeOps = []StoreOptionFunc{
		StoreWithBigtableClient(btc),
		StoreWithTableNamePrefix(cfg.Bigtable.TablePrefix),
		StoreWithLogger(s.Logger),
		StoreWithHashLabels(cfg.HashMetricName),
		StoreWithEnableMetrics(cfg.Metric.Enable),
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

	if cfg.EnableLongtermStorage {
		lbtcops, lbtc, err := getBTOpts(cfg.LongtermBigtable)
		if err != nil {
			return nil, err
		}

		storeOps = append(storeOps,
			StoreWithEnableLongtermStorage(cfg.EnableLongtermStorage),
			StoreWithLongermBigtableClient(lbtc),
			StoreWithLongtermTableNamePrefix(cfg.LongtermBigtable.TablePrefix),
			// StoreWithLong
		)

		if cfg.EnsureTables {
			lbtac, err := bigtable.NewAdminClient(
				context.Background(),
				cfg.LongtermBigtable.ProjectID,
				cfg.LongtermBigtable.Instance,
				lbtcops...)
			if err != nil {
				return nil, err
			}
			storeOps = append(storeOps, StoreWithLongtermBigtableAdminClient(lbtac))
		}
	}

	store, err := NewStore(storeOps...)
	if err != nil {
		return nil, err
	}
	s.store = store

	if cfg.EnsureTables {
		if err := s.store.EnsureTables(context.Background()); err != nil {
			return nil, err
		}
	}

	s.mux.Handle("/write", http.HandlerFunc(s.HandleWrite))
	s.mux.Handle("/read", http.HandlerFunc(s.HandleRead))

	lis, err := net.Listen("tcp", cfg.Web.Listen)
	if err != nil {
		return nil, err
	}
	s.l = lis

	if cfg.Metric.Enable {
		prometheusListenStr := cfg.Metric.Listen
		if prometheusListenStr == "" {
			prometheusListenStr = "127.0.0.1:9100"
		}
		pLis, err := net.Listen("tcp", prometheusListenStr)
		if err != nil {
			return nil, err
		}
		s.plistener = pLis
		s.psrv = &http.Server{
			Handler: promhttp.Handler(),
		}
	}

	return s, nil
}

// HandleWrite -
func (s *Server) HandleWrite(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.Logger.Error("Read error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		s.Logger.Error("Decode error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req = new(prompb.WriteRequest)
	if err := req.Unmarshal(reqBuf); err != nil {
		s.Logger.Error("Unmarshal error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.store.Put(r.Context(), req); err != nil {
		s.Logger.Error("Put error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleRead -
func (s *Server) HandleRead(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.Logger.Error("Read error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		s.Logger.Error("Decode error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req = new(prompb.ReadRequest)
	if err := req.Unmarshal(reqBuf); err != nil {
		s.Logger.Error("Unmarshal error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.Logger.Info("Read", zap.String("req.string", req.String()))
	res, err := s.store.Read(r.Context(), req)
	if err != nil {
		s.Logger.Error("Read error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := res.Marshal()
	if err != nil {
		s.Logger.Error("Marshal error", zap.Error(err))
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

	// http prometheus metric server
	if s.psrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer wgCancel() // signal other servers to close too.
			if err := s.psrv.Serve(s.plistener); err != nil {
				s.Logger.Error("http prometheus metric server exited with error", zap.Error(err))
			}
		}()
		s.Logger.Info("prometheus metric http server started", zap.String("Listen", s.cfg.Metric.Listen))
	}

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

	if s.psrv != nil {
		if err := s.psrv.Shutdown(ctx); err != nil {
			s.Logger.Warn("close http prometheus metric server error", zap.Error(err))
		}
	}

	if err := s.store.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return fmt.Errorf("HTTPServer.Close with errors: %v", errs)
	}
	return nil
}
