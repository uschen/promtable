package promtable

import (
	"context"
	"errors"
	"sync"

	"cloud.google.com/go/bigtable"
	"github.com/uschen/promtable/prompb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	metricFamily = "m"
	// MetricNameLabel is the label name indicating the metric name of a
	// timeseries.
	MetricNameLabel = "__name__"
	// DefaultTableName -
	DefaultTableName = "metrics"
)

// Store - bigtable store
type Store struct {
	c *bigtable.Client

	adminClient *bigtable.AdminClient

	tableName string
	tbl       *bigtable.Table

	logger *zap.Logger
}

// StoreOptionFunc -
type StoreOptionFunc func(*Store) error

// NewStore -
func NewStore(options ...StoreOptionFunc) (*Store, error) {
	var s = new(Store)

	for i := range options {
		if err := options[i](s); err != nil {
			return nil, err
		}
	}

	if s.c == nil {
		return nil, errors.New("bigtable client is required")
	}

	if s.tableName == "" {
		s.tableName = DefaultTableName
	}

	if s.logger == nil {
		s.logger = zap.NewNop()
	}

	s.tbl = s.c.Open(s.tableName)
	return s, nil
}

// StoreWithLogger -
func StoreWithLogger(l *zap.Logger) StoreOptionFunc {
	return func(s *Store) error {
		s.logger = l
		return nil
	}
}

// StoreWithTableName -
func StoreWithTableName(name string) StoreOptionFunc {
	return func(s *Store) error {
		s.tableName = name
		return nil
	}
}

// StoreWithBigtableClient -
func StoreWithBigtableClient(c *bigtable.Client) StoreOptionFunc {
	return func(s *Store) error {
		s.c = c
		return nil
	}
}

// StoreWithBigtableAdminClient -
func StoreWithBigtableAdminClient(ac *bigtable.AdminClient) StoreOptionFunc {
	return func(s *Store) error {
		s.adminClient = ac
		return nil
	}
}

// EnsureTables -
func (s *Store) EnsureTables(ctx context.Context) error {
	if s.adminClient == nil {
		return errors.New("EnsureTables requires adminClient")
	}

	if err := s.createTableIfNotExist(ctx, s.tableName); err != nil {
		return err
	}

	// create column family
	if err := s.createColumnFamilyIfNotExist(ctx, s.tableName, metricFamily); err != nil {
		return err
	}
	return nil
}

// Put -
func (s *Store) Put(ctx context.Context, namespace string, req *prompb.WriteRequest) error {
	var rows []*TimeseriesRow
	for i := range req.Timeseries {
		rs, err := RowsFromTimeseries(namespace, req.Timeseries[i])
		if err != nil {
			return err
		}
		rows = append(rows, rs...)
	}
	return s.WriteRows(ctx, rows)
}

// Read -
func (s *Store) Read(ctx context.Context, namespace string, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	res := new(prompb.ReadResponse)
	if len(req.Queries) == 0 {
		return res, nil
	}
	var wg sync.WaitGroup

	var qrs = make([]*prompb.QueryResult, len(req.Queries))
	// var tsm = make(map[MetricIdentifier][]*prompb.TimeSeries)

	for i := range req.Queries {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			ts, err := s.Query(ctx, namespace, req.Queries[k])
			if err != nil {
				return
			}
			qrs[k] = new(prompb.QueryResult)
			for _, v := range ts {
				qrs[k].Timeseries = append(qrs[k].Timeseries, v)
			}
		}(i)
	}

	wg.Wait()

	res.Results = qrs
	return res, nil
}

// Query can result into more than one TimeSeries
func (s *Store) Query(ctx context.Context, namespace string, q *prompb.Query) ([]*prompb.TimeSeries, error) {
	rr, ok := QueryToBigtableRowRange(namespace, q)
	if !ok {
		return nil, nil
	}

	var filters = []bigtable.Filter{
		bigtable.FamilyFilter(metricFamily),
	}

	cf := QueryToBigtableColumnFilter(q)
	if cf != nil {
		filters = append(filters, cf)
	}

	filters = append(filters, bigtable.LatestNFilter(1))

	rkFilter := QueryToRowKeyFilter(q)

	var ts = make(map[MetricIdentifier]*prompb.TimeSeries)

	err := s.tbl.ReadRows(ctx, rr, func(r bigtable.Row) bool {
		name, metricID, labels, ok := rkFilter(r.Key())
		if !ok {
			return true
		}
		ss, err := BtRowToPromSamples(r)
		if err != nil {
			s.logger.Warn("BtRowToPromSamples error", zap.Error(err))
			// TODO: handle error
			return true
		}
		if ts[metricID] == nil {
			ts[metricID] = &prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: MetricNameLabel, Value: name},
				},
			}
			ts[metricID].Labels = append(ts[metricID].Labels, labels...)
		}
		ts[metricID].Samples = append(ts[metricID].Samples, ss...)
		return true
	}, bigtable.RowFilter(
		bigtable.ChainFilters(
			filters...,
		),
	))
	if err != nil {
		return nil, err
	}
	var res []*prompb.TimeSeries
	for _, v := range ts {
		res = append(res, v)
	}
	return res, nil
}

// WriteRows -
func (s *Store) WriteRows(ctx context.Context, rows []*TimeseriesRow) error {
	var rks []string
	var muts []*bigtable.Mutation
	for i := range rows {
		rks = append(rks, rows[i].RowKey)
		mut := bigtable.NewMutation()
		for ts, value := range rows[i].Values {
			mut.Set(metricFamily, string(Int64ToBytes(ts)), bigtable.Timestamp(ts*1000), value)
		}
		muts = append(muts, mut)
	}
	// TODO: deal with partial fail
	if _, err := s.tbl.ApplyBulk(ctx, rks, muts); err != nil {
		return err
	}
	return nil
}

func (s *Store) createTableIfNotExist(ctx context.Context, table string) error {
	_, err := s.adminClient.TableInfo(ctx, table)
	if err != nil {
		if grpc.Code(err) != codes.NotFound {
			return err
		}
		// create table.
		if err2 := s.adminClient.CreateTable(ctx, table); err2 != nil {
			return err2
		}
	}
	return nil
}

func (s *Store) createColumnFamilyIfNotExist(ctx context.Context, table string, family string) error {
	if err := s.adminClient.CreateColumnFamily(ctx, table, family); err != nil {
		if grpc.Code(err) != codes.AlreadyExists {
			return err
		}
		return nil
	}
	return nil
}

// BtRowToPromSamples -
func BtRowToPromSamples(r bigtable.Row) ([]prompb.Sample, error) {
	const columnPrefix = metricFamily + ":"
	var samples = make([]prompb.Sample, len(r[metricFamily]))

	// fill out samples
	for i, v := range r[metricFamily] {
		samples[i] = prompb.Sample{
			Value:     Float64FromBytes(v.Value),
			Timestamp: int64(v.Timestamp) / 1000,
		}
	}
	return samples, nil
}

// BtRowToPromTimeSeries -
func BtRowToPromTimeSeries(r bigtable.Row, metricName string, labels Labels) (*prompb.TimeSeries, error) {
	t := &prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: MetricNameLabel, Value: metricName},
		},
	}
	t.Labels = append(t.Labels, labels...)

	const columnPrefix = metricFamily + ":"

	// fill out samples
	for _, v := range r[metricFamily] {
		l := prompb.Sample{
			Value:     Float64FromBytes(v.Value),
			Timestamp: int64(v.Timestamp) / 1000,
		}
		t.Samples = append(t.Samples, l)
	}

	return t, nil
}
