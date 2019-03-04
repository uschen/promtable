package promtable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/bigtable"
	"github.com/uschen/promtable/prompb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	metricFamily                 = "m"
	metricFamilyPrefixLen        = len(metricFamily + ":")
	indexRowLabelFamily          = "l"
	indexRowLabelColumnPrefixLen = len(indexRowLabelFamily + ":")
	// MetricNameLabel is the label name indicating the metric name of a
	// timeseries.
	MetricNameLabel = "__name__"
	// DefaultTableName -
	DefaultTableName = "metrics"
	// DefaultMetricMetaTableName -
	DefaultMetricMetaTableName = "metrics_meta"
	// DefaultBucketSizeHours -
	DefaultBucketSizeHours int64 = 24 // 24 hours

	// DefaultBucketSizeMilliSeconds -
	DefaultBucketSizeMilliSeconds int64 = 24 * 60 * 60 * 1000
)

var (
	errUnImpl = errors.New("not implemented")
)

// Store - bigtable store
type Store struct {
	c *bigtable.Client

	adminClient *bigtable.AdminClient

	tablePrefix string

	tableName     string
	metaTableName string

	tbl  *bigtable.Table
	mtbl *bigtable.Table

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

	if s.tablePrefix == "" {
		return nil, errors.New("table prefix is required")
	}

	if s.tableName == "" {
		s.tableName = s.tablePrefix + DefaultTableName
	}
	if s.metaTableName == "" {
		s.metaTableName = s.tablePrefix + DefaultMetricMetaTableName
	}

	if s.logger == nil {
		s.logger = zap.NewNop()
	}

	s.tbl = s.c.Open(s.tableName)
	s.mtbl = s.c.Open(s.metaTableName)
	return s, nil
}

// StoreWithLogger -
func StoreWithLogger(l *zap.Logger) StoreOptionFunc {
	return func(s *Store) error {
		s.logger = l
		return nil
	}
}

// StoreWithTableNamePrefix -
func StoreWithTableNamePrefix(prefix string) StoreOptionFunc {
	return func(s *Store) error {
		s.tablePrefix = prefix
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
	if err := s.adminClient.SetGCPolicy(ctx, s.tableName, metricFamily, bigtable.MaxVersionsPolicy(1)); err != nil {
		return err
	}

	if err := s.createTableIfNotExist(ctx, s.metaTableName); err != nil {
		return err
	}
	if err := s.createColumnFamilyIfNotExist(ctx, s.metaTableName, indexRowLabelFamily); err != nil {
		return err
	}
	if err := s.adminClient.SetGCPolicy(ctx, s.metaTableName, indexRowLabelFamily, bigtable.MaxVersionsPolicy(1)); err != nil {
		return err
	}
	return nil
}

// Put -
// for each Timeseries, prepare the metric_name, labelsString first.
func (s *Store) Put(ctx context.Context, req *prompb.WriteRequest) error {
	// map[<metric_name>#<labelsString>#<base>][]prompb.Sample

	// write metrics first
	var buckets = make(map[string][]prompb.Sample) // map<metric_rowkey>[]prompb.Sample
	var metaBuckets = make(map[string][]string)    // map<meta_rowkey>labelsString
	for i := range req.Timeseries {
		// metrics
		ts := req.Timeseries[i]
		sort.Slice(ts.Labels, func(i, j int) bool {
			return ts.Labels[i].Name < ts.Labels[j].Name
		})
		name, labelsString, err := LabelsToRowKeyComponents(ts.Labels)
		if err != nil {
			return err
		}

		var baseBucket = make(map[int64][]prompb.Sample)
		for k := range ts.Samples {
			base := ts.Samples[k].Timestamp / DefaultBucketSizeMilliSeconds
			baseBucket[base] = append(baseBucket[base], ts.Samples[k])
		}

		rkPrefix := name + "#" + labelsString + "#"
		for base, samples := range baseBucket {
			// metrics
			var rkBuilder strings.Builder
			rkBuilder.WriteString(rkPrefix)
			rkBuilder.Write(Int64ToBytes(base))
			rk := rkBuilder.String()
			buckets[rk] = append(buckets[rk], samples...)
			// meta
			var mrkBuilder strings.Builder
			mrkBuilder.WriteString(name)
			mrkBuilder.WriteRune('#')
			mrkBuilder.Write(Int64ToBytes(base))
			metaRK := mrkBuilder.String()

			metaBuckets[metaRK] = append(metaBuckets[metaRK], labelsString)
		}
	}

	// prepare the metrics
	var (
		rks  = make([]string, len(buckets))
		muts = make([]*bigtable.Mutation, len(buckets))
		k    int
	)
	for rk, samples := range buckets {
		rks[k] = rk
		mut := bigtable.NewMutation()
		for i := range samples {
			mut.Set(
				metricFamily,
				string(Int64ToBytes(samples[i].Timestamp)), // column
				bigtable.Timestamp(samples[i].Timestamp*1e3),
				Float64ToBytes(samples[i].Value), // value
			)
		}
		muts[k] = mut
		k++
	}

	// prepare the meta
	var (
		irks   = make([]string, len(metaBuckets))
		irmuts = make([]*bigtable.Mutation, len(metaBuckets))
		ts     = bigtable.Now()
		i      int
	)

	for rk, labelsStrings := range metaBuckets {
		irks[i] = rk
		mut := bigtable.NewMutation()
		for j := range labelsStrings {
			mut.Set(indexRowLabelFamily, labelsStrings[j], ts, nil)
		}
		irmuts[i] = mut
		i++
	}

	if len(rks) > 0 {
		if _, err := s.tbl.ApplyBulk(ctx, rks, muts); err != nil {
			// TODO: deal with partial fail
			return err
		}
	}

	if len(irks) > 0 {
		if _, err := s.mtbl.ApplyBulk(ctx, irks, irmuts); err != nil {
			return err
		}
	}

	return nil
}

// Read -
func (s *Store) Read(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
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
			// sort label matcher
			// sort.Slice(req.Queries[k].Matchers, func(i, j int) bool {
			// 	return req.Queries[k].Matchers[i].Name < req.Queries[k].Matchers[j].Name
			// })
			ts, err := s.Query(ctx, req.Queries[k])
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
	for i := range qrs {
		if qrs[i] == nil {
			continue
		}
		res.Results = append(res.Results, qrs[i])
	}

	return res, nil
}

// Query performs read metrics against single prompb.Query
// It will first query the meta table to find out exactly which metric rows (ranges) are needed
// to fetch.
func (s *Store) Query(ctx context.Context, q *prompb.Query) ([]*prompb.TimeSeries, error) {
	// each SeriesRange represents a unique Timeseries
	srs, err := s.QueryMetaRows(ctx, q)
	if err != nil {
		return nil, err
	}

	if len(srs) == 0 {
		return []*prompb.TimeSeries{}, nil
	}
	var (
		name                  = srs[0].Name
		rrl                   = make(bigtable.RowRangeList, len(srs))
		seriesRangeBucket     = make(map[string][]prompb.Label) // map<labelsString>[]prompb.Label
		sampleLabelsStrings   []string
		sampleBuckets         = make(map[string][]prompb.Sample)
		expectedNamePrefixLen = len(name) + 1
		expectedMinRowKeyLen  = len(name) + 2 + 8 // 2 '#' + 8 bytes base
	)
	for i := range srs {
		rrl[i] = NewMetricRowRange(srs[i].Name, srs[i].LabelsString, srs[i].BaseStart, srs[i].BaseEnd)
		seriesRangeBucket[srs[i].LabelsString] = srs[i].Labels
	}

	var filters = []bigtable.Filter{
		bigtable.FamilyFilter(metricFamily),
	}

	if cf := QueryToBigtableColumnFilter(q.StartTimestampMs, q.EndTimestampMs); cf != nil {
		filters = append(filters, cf)
	}

	filters = append(filters, bigtable.LatestNFilter(1))

	var readErr error
	err = s.tbl.ReadRows(ctx, rrl, func(r bigtable.Row) bool {
		// name, metricID, labels, ok := rkFilter(r.Key())
		// if !ok {
		// 	return true
		// }
		ss := BtRowToPromSamples(r)
		// rowkey: <metric_name>#<labelsString>#base
		rk := r.Key()
		if len(rk) < expectedMinRowKeyLen {
			readErr = errors.New("invalid metric row key read: '" + rk + "'")
			return false
		}
		labelsString := rk[expectedNamePrefixLen : len(rk)-9]
		if _, ok := sampleBuckets[labelsString]; !ok {
			// maintain order
			sampleLabelsStrings = append(sampleLabelsStrings, labelsString)
		}
		sampleBuckets[labelsString] = append(sampleBuckets[labelsString], ss...)
		return true
	}, bigtable.RowFilter(
		bigtable.ChainFilters(
			filters...,
		),
	))
	if err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, readErr
	}

	var res = make([]*prompb.TimeSeries, len(sampleLabelsStrings))
	for i := range sampleLabelsStrings {
		ts := &prompb.TimeSeries{
			Labels:  append([]prompb.Label{{Name: MetricNameLabel, Value: name}}, seriesRangeBucket[sampleLabelsStrings[i]]...),
			Samples: sampleBuckets[sampleLabelsStrings[i]],
		}
		res[i] = ts
		i++
	}
	return res, nil
}

// SeriesRange - represent a continues row range for a series.
// this allows pinpoint read of metric of the series.
type SeriesRange struct {
	// StartMs and EndMs are for filter metric sample columns.
	StartMs, EndMs int64
	// BaseStart and BaseEnd are for filter rowkey of metric samples.
	BaseStart, BaseEnd string
	Name, LabelsString string
	Labels             []prompb.Label
}

// Stirng -
func (s *SeriesRange) Stirng() string {
	return fmt.Sprintf("SeriesRange: [StartMs: %d, EndMs: %d, BaseStart: %x, BaseEnd: %x, Name: %s, LabelsString: %s, Labels: %v", s.StartMs, s.EndMs, s.BaseStart, s.BaseEnd, s.Name, s.LabelsString, s.Labels)
}

// NewMetricRowRange -
func NewMetricRowRange(name, labelsString string, baseStart, baseEnd string) bigtable.RowRange {
	rkPrefix := name + "#" + labelsString + "#"
	if baseStart == "" && baseEnd == "" {
		rr := bigtable.PrefixRange(rkPrefix)
		return rr
	}
	var (
		begin, end string
	)
	begin = rkPrefix + baseStart
	if baseEnd == "" {
		end = prefixSuccessor(rkPrefix)
	} else {
		end = rkPrefix + prefixSuccessor(baseEnd)
	}
	rr := bigtable.NewRange(begin, end)
	return rr
}

type metaRowBaseWithColumn struct {
	base         string
	labelsString string
}

// QueryMetaRows - query metric meta index and return array of MetaRow which can be used to
// generate metric baseRange based name#labelsString
func (s *Store) QueryMetaRows(ctx context.Context, q *prompb.Query) ([]SeriesRange, error) {
	var (
		err  error
		name string
	)
	sort.Slice(q.Matchers, func(i, j int) bool {
		return q.Matchers[i].Name < q.Matchers[j].Name
	})
	if len(q.Matchers) == 0 || q.Matchers[0].Name != MetricNameLabel {
		return nil, errors.New("query without metric name is not supported")
	}
	name = q.Matchers[0].Value

	rr, err := QueryToBigtableMetaRowRange(name, q.StartTimestampMs, q.EndTimestampMs)
	if err != nil {
		return nil, err
	}

	var (
		readErr error

		rkPrefixLen = len(name + "#")
		rkLen       = rkPrefixLen + 8 // + uint64

		metaRows []metaRowBaseWithColumn

		filters = []bigtable.Filter{
			bigtable.FamilyFilter(indexRowLabelFamily),
			bigtable.LatestNFilter(1),
			bigtable.StripValueFilter(),
		}
	)
	err = s.mtbl.ReadRows(ctx, rr, func(r bigtable.Row) bool {
		rk := r.Key()
		if len(rk) != rkLen {
			readErr = errors.New("invalid rowkey length '" + rk + "'")
			return false
		}
		baseStr := rk[rkPrefixLen:]
		// base := Int64FromBytes([]byte(baseStr))

		for _, c := range r[indexRowLabelFamily] {
			if len(c.Column) < indexRowLabelColumnPrefixLen {
				readErr = errors.New("invalid column length '" + c.Column + "', for row: '" + rk + "'")
				return false
			}
			if len(c.Column) >= indexRowLabelColumnPrefixLen {
				labelsString := c.Column[indexRowLabelColumnPrefixLen:]
				metaRows = append(metaRows, metaRowBaseWithColumn{
					base:         baseStr,
					labelsString: labelsString,
				})
			}
		}
		return true
	}, bigtable.RowFilter(
		bigtable.ChainFilters(
			filters...,
		),
	))
	if err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, err
	}
	// convert metaRows into SeriesRange and filter out which metaRowBaseWithColumn are actually appliable to the query.
	lsm, err := QueryMatchersToLabelsMatcher(q.Matchers[1:])
	if err != nil {
		return nil, err
	}

	var (
		matchedLabels = make(map[string][]prompb.Label) // map<labelsString>[]prombpb.Label
		labelsBases   = make(map[string][]string)       // map<labelsString>[]<base>
	)

	for i := range metaRows {
		if _, ok := matchedLabels[metaRows[i].labelsString]; ok {
			// already matched
			// TODO: is it possible to have duplicated base for the same lables?
			labelsBases[metaRows[i].labelsString] = append(labelsBases[metaRows[i].labelsString], metaRows[i].base)
			continue
		}
		labels, err := LabelsFromString(metaRows[i].labelsString)
		if err != nil {
			return nil, err
		}
		// TODO: it is possible to combine the filter and parts in one loop, since in the filter, labels are looped.
		if !lsm.Match(labels) {
			// labels doesn't match
			continue
		}

		matchedLabels[metaRows[i].labelsString] = labels
		labelsBases[metaRows[i].labelsString] = append(labelsBases[metaRows[i].labelsString], metaRows[i].base)
	}
	// for each matched labels, generate one SeriesRange
	var (
		res = make([]SeriesRange, len(matchedLabels))
		i   int
	)
	for k, lables := range matchedLabels {
		res[i] = SeriesRange{
			StartMs:      q.StartTimestampMs,
			EndMs:        q.EndTimestampMs,
			BaseStart:    labelsBases[k][0],
			BaseEnd:      labelsBases[k][len(labelsBases[k])-1],
			Name:         name,
			LabelsString: k,
			Labels:       lables,
		}
		i++
	}
	return res, nil
}

// QueryToBigtableMetaRowRange -
func QueryToBigtableMetaRowRange(name string, startTs, endTs int64) (bigtable.RowSet, error) {
	prefix := name + "#"
	if startTs == 0 && endTs == 0 {
		return bigtable.PrefixRange(prefix), nil
	}
	var (
		begin, end string
	)
	// build <metric_name>#<base_start> ...
	begin = prefix + string(Int64ToBytes(startTs/DefaultBucketSizeMilliSeconds))
	if endTs == 0 {
		end = prefixSuccessor(prefix)
	} else {
		end = prefix + string(Int64ToBytes(endTs/DefaultBucketSizeMilliSeconds+1))
	}
	return bigtable.NewRange(begin, end), nil
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

// QueryToBigtableColumnFilter -
func QueryToBigtableColumnFilter(startMs, endMs int64) bigtable.Filter {
	if startMs == 0 && endMs == 0 {
		return nil
	}
	var begin, end string
	if startMs != 0 {
		begin = string(Int64ToBytes(startMs))
	}
	if endMs != 0 {
		end = string(Int64ToBytes(endMs + 1))
	}
	return bigtable.ColumnRangeFilter(metricFamily, begin, end)
}

// BtRowToPromSamples -
func BtRowToPromSamples(r bigtable.Row) []prompb.Sample {
	var samples = make([]prompb.Sample, len(r[metricFamily]))

	// fill out samples
	for i, v := range r[metricFamily] {
		samples[i] = prompb.Sample{
			Value:     Float64FromBytes(v.Value),
			Timestamp: int64(v.Timestamp) / 1000,
		}
	}
	return samples
}

// prefixSuccessor returns the lexically smallest string greater than the
// prefix, if it exists, or "" otherwise.  In either case, it is the string
// needed for the Limit of a RowRange.
func prefixSuccessor(prefix string) string {
	if prefix == "" {
		return "" // infinite range
	}
	n := len(prefix)
	for n--; n >= 0 && prefix[n] == '\xff'; n-- {
	}
	if n == -1 {
		return ""
	}
	ans := []byte(prefix[:n])
	ans = append(ans, prefix[n]+1)
	return string(ans)
}
