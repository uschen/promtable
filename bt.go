package promtable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/twmb/murmur3"
	"github.com/uschen/promtable/pkg/btrowset"
	"github.com/uschen/promtable/prompb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
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

	// DefaultLongtermTableName -
	DefaultLongtermTableName = "metrics"

	// DefaultMetricMetaTableName -
	DefaultMetricMetaTableName = "metrics_meta"
	// DefaultBucketSizeHours -
	DefaultBucketSizeHours int64 = 24 // 24 hours

	// DefaultBucketSizeMilliSeconds - 24 hours.
	DefaultBucketSizeMilliSeconds int64 = 60 * 60 * 1000 // 1hr

	// DefaultBucketSizeMilliSeconds int64 = 24 * 60 * 60 * 1000

	hashedMetricRowKeySuffixLen = 1 + 8 + 16                      // `#` + `128bits` + `Int64`
	minHashedMetricRowkeyLen    = hashedMetricRowKeySuffixLen + 1 //

	colLen     = 4 + 8                          // 4 bytes for offset + 8 bytes for float64
	fullColLen = metricFamilyPrefixLen + colLen // 2 + 4 + 8
)

var (
	errUnImpl = errors.New("not implemented")

	zeroBaseStart = string(Int64ToBytes(0))
	maxBaseEnd    = string(Int64ToBytes(math.MaxInt64))
)

// Store - bigtable store
type Store struct {
	c *bigtable.Client

	adminClient *bigtable.AdminClient

	tablePrefix string

	tableName     string
	metaTableName string

	metricExpiration time.Duration

	tbl  *bigtable.Table
	mtbl *bigtable.Table

	metaWriteCache     sync.Map
	metaCacheSizeBytes uint64

	ctx    context.Context
	cancel context.CancelFunc

	hashLabels bool

	metrics *StoreMetrics

	logger *zap.Logger

	// long term storage bigtable
	enableLongterm bool
	lc             *bigtable.Client
	lac            *bigtable.AdminClient
	ltablePrefix   string
	ltableName     string
	ltbl           *bigtable.Table
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

	if s.hashLabels {
		s.logger.Info("Store will store metric with hashed rowkey")
	}

	s.tbl = s.c.Open(s.tableName)
	s.mtbl = s.c.Open(s.metaTableName)

	// long term
	if s.enableLongterm {
		if s.ltablePrefix == "" {
			return nil, errors.New("long term table prefix is required")
		}
		if s.ltableName == "" {
			s.ltableName = s.ltablePrefix + DefaultLongtermTableName
		}
		if s.lc == nil {
			return nil, errors.New("long term bigtable client is required")
		}

		s.ltbl = s.lc.Open(s.ltableName)
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	go s.RunMetaCacheGC()

	return s, nil
}

// Close -
func (s *Store) Close() error {
	s.cancel()
	return nil
}

// StoreWithLogger -
func StoreWithLogger(l *zap.Logger) StoreOptionFunc {
	return func(s *Store) error {
		s.logger = l
		return nil
	}
}

// StoreWithEnableLongtermStorage -
func StoreWithEnableLongtermStorage(enabled bool) StoreOptionFunc {
	return func(s *Store) error {
		s.enableLongterm = enabled
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

// StoreWithLongtermTableNamePrefix -
func StoreWithLongtermTableNamePrefix(prefix string) StoreOptionFunc {
	return func(s *Store) error {
		s.ltablePrefix = prefix
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

// StoreWithLongermBigtableClient -
func StoreWithLongermBigtableClient(c *bigtable.Client) StoreOptionFunc {
	return func(s *Store) error {
		s.lc = c
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

// StoreWithLongtermBigtableAdminClient -
func StoreWithLongtermBigtableAdminClient(ac *bigtable.AdminClient) StoreOptionFunc {
	return func(s *Store) error {
		s.lac = ac
		return nil
	}
}

// StoreWithMetricExpiration -
func StoreWithMetricExpiration(exp time.Duration) StoreOptionFunc {
	return func(s *Store) error {
		s.metricExpiration = exp
		return nil
	}
}

// StoreWithHashLabels -
func StoreWithHashLabels(hash bool) StoreOptionFunc {
	return func(s *Store) error {
		s.hashLabels = hash
		return nil
	}
}

// StoreWithEnableMetrics -
func StoreWithEnableMetrics(enabled bool) StoreOptionFunc {
	return func(s *Store) error {
		s.metrics = NewStoreMetrics()
		return nil
	}
}

// EnsureTables -
func (s *Store) EnsureTables(ctx context.Context) error {
	if s.adminClient == nil {
		return errors.New("EnsureTables requires adminClient")
	}

	if s.enableLongterm && s.lac == nil {
		return errors.New("EnsureTables requires long term adminClient")
	}

	if err := s.createTableIfNotExist(ctx, s.adminClient, s.tableName); err != nil {
		return err
	}

	var (
		metricPolicy bigtable.GCPolicy
	)

	if s.metricExpiration == 0 {
		metricPolicy = bigtable.MaxVersionsPolicy(1)
	} else {
		metricPolicy = bigtable.UnionPolicy(
			bigtable.MaxVersionsPolicy(1),
			bigtable.MaxAgePolicy(s.metricExpiration),
		)
	}

	// create column family
	// metric
	if err := s.createColumnFamilyIfNotExist(ctx, s.adminClient, s.tableName, metricFamily); err != nil {
		return err
	}
	if err := s.adminClient.SetGCPolicy(ctx, s.tableName, metricFamily, metricPolicy); err != nil {
		return err
	}

	// meta
	if err := s.createTableIfNotExist(ctx, s.adminClient, s.metaTableName); err != nil {
		return err
	}
	if err := s.createColumnFamilyIfNotExist(ctx, s.adminClient, s.metaTableName, indexRowLabelFamily); err != nil {
		return err
	}
	if err := s.adminClient.SetGCPolicy(ctx, s.metaTableName, indexRowLabelFamily, bigtable.MaxVersionsPolicy(1)); err != nil {
		return err
	}

	if s.enableLongterm {
		if err := s.createTableIfNotExist(ctx, s.lac, s.ltableName); err != nil {
			return err
		}
		if err := s.createColumnFamilyIfNotExist(ctx, s.lac, s.ltableName, metricFamily); err != nil {
			return err
		}
		if err := s.lac.SetGCPolicy(ctx, s.ltableName, metricFamily, bigtable.MaxVersionsPolicy(1)); err != nil {
			return err
		}
	}
	return nil
}

// Put -
// for each Timeseries, prepare the metric_name, labelsString first.
//
// RowKey: <name>#<base><hashed labels>
//   * name will be raw name
//   * base will be encoded into 8 bytes
//   * hashed labels will be using murmur3 128 (128 bits, 16 byes)
//
// Column Qualifier: <timestamp reminder><value>
//   * 4 bytes timestamp reminder
//   * 8 bytes
//
// Column Value: nil
// Column Timestamp: sample timestamp
//
// If longterm storage is enabled, Put will write to both storage.
func (s *Store) Put(ctx context.Context, req *prompb.WriteRequest) error {
	// write metrics first
	s.metrics.IncPutTimeseriesCount(len(req.Timeseries))

	var (
		rks      []string
		muts     []*bigtable.Mutation
		metaMuts = make(map[string]*bigtable.Mutation) // map<meta_rowkey>labelsString

		h128 = murmur3.New128() // labelsString hasher
		buf  bytes.Buffer       // buf will be reused to construct row key.
	)

	for i := range req.Timeseries {
		s.metrics.IncPutSampleCount(len(req.Timeseries[i].Samples))
		// metrics
		ts := req.Timeseries[i]
		sort.Slice(ts.Labels, func(i, j int) bool {
			return ts.Labels[i].Name < ts.Labels[j].Name
		})
		name, labelsString, err := LabelsToRowKeyComponents(ts.Labels)
		if err != nil {
			return err
		}

		var (
			baseMutations = make(map[int64]*bigtable.Mutation)
			hashedLabels  []byte
		)

		if s.hashLabels {
			h128.Reset()
			h128.Write([]byte(labelsString))
			hashedLabels = h128.Sum(nil)
		} else {
			// rkPrefix = rkPrefix + "#"
		}

		for k := range ts.Samples {
			var (
				col    = make([]byte, colLen)
				base   = ts.Samples[k].Timestamp / DefaultBucketSizeMilliSeconds
				offset = int32(ts.Samples[k].Timestamp - base*DefaultBucketSizeMilliSeconds)
			)
			// check if base is already there.
			if _, ok := baseMutations[base]; !ok {
				// not yet,
				baseMutations[base] = bigtable.NewMutation()
			}
			copy(col[:4], Int32ToBytes(offset))
			copy(col[4:], Float64ToBytes(ts.Samples[k].Value))
			// metric value (64 bits) will be stored as part of the column qualifier.
			// col: <timestamp
			baseMutations[base].Set(
				metricFamily,
				string(col), // column
				bigtable.Timestamp(ts.Samples[k].Timestamp*1e3),
				nil,
			)
		}

		// construct row keys
		var (
			t = bigtable.Now()
		)
		for base, mut := range baseMutations {
			buf.Reset()
			// metrics
			buf.WriteString(name)
			buf.WriteRune('#')
			buf.Write(hashedLabels)
			buf.Write(Int64ToBytes(base))

			rk := buf.String()
			rks = append(rks, rk)
			muts = append(muts, mut)

			// check if meta is already written, caching is use metrics rowkey
			if _, ok := s.metaWriteCache.LoadOrStore(rk, time.Now()); !ok {
				// meta
				// meta row key will be: <name>#<base>
				buf.Reset()
				buf.WriteString(name)
				buf.WriteRune('#')
				buf.Write(Int64ToBytes(base))
				metaRK := buf.String()
				if _, ok := metaMuts[metaRK]; !ok {
					metaMuts[metaRK] = bigtable.NewMutation()
				}
				metaMuts[metaRK].Set(indexRowLabelFamily, labelsString, t, nil)
			}
		}
	}

	// prepare the meta
	var (
		irks   = make([]string, len(metaMuts))
		irmuts = make([]*bigtable.Mutation, len(metaMuts))

		i int
	)

	for rk, mut := range metaMuts {
		irks[i] = rk
		irmuts[i] = mut
		i++
	}

	if len(rks) > 0 {
		// write metrics
		if _, err := s.tbl.ApplyBulk(ctx, rks, muts); err != nil {
			// TODO: deal with partial fail
			return err
		}
		if s.enableLongterm {
			if _, err := s.ltbl.ApplyBulk(ctx, rks, muts); err != nil {
				return err
			}
		}
	}

	if len(irks) > 0 {
		// write meta
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

	for i := range req.Queries {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			// TODO: sort?
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
	s.metrics.IncQueryMetricTimeseriesReadCount(len(res.Results))
	return res, nil
}

// Query performs read metrics against single prompb.Query
// It will first query the meta table to find out exactly which metric rows (ranges) are needed
// to fetch.
//
// If metricExpiry is enabled, the query's timestamp range will be truncated.
// if metricExpiry is enabled and longterm storage is enabled, query will be send to separate places.
func (s *Store) Query(ctx context.Context, q *prompb.Query) ([]*prompb.TimeSeries, error) {
	var (
		qq       *prompb.Query = &(*q)
		expiryMs int64
	)

	if s.metricExpiration != 0 {
		expiryMs = TimeMs(time.Now().Add(-s.metricExpiration))
		if !s.enableLongterm {
			// if long term is not enabled
			// simply truncate the timestamp to the query meta rows
			if qq.EndTimestampMs < expiryMs { // start is expired
				return nil, nil
			}
			if qq.StartTimestampMs < expiryMs {
				qq.StartTimestampMs = expiryMs
			}
		}
	}
	// each SeriesRange represents a unique Timeseries
	srs, err := s.QueryMetaRows(ctx, qq)
	if err != nil {
		return nil, err
	}

	if len(srs) == 0 {
		return []*prompb.TimeSeries{}, nil
	}

	s.metrics.IncQuerySeriesRangesCount(len(srs))

	var (
		name                = srs[0].Name
		rrl                 []*btpb.RowRange
		lrrl                []*btpb.RowRange                  // long term row range if expiration is set and longterm is enabled.
		seriesRangeBucket   = make(map[string][]prompb.Label) // map<labelsString>[]prompb.Label
		sampleLabelsStrings []string
		sampleBuckets       = make(map[string][]prompb.Sample)

		// buf  bytes.Buffer
		h128 = murmur3.New128()
	)
	// convert SeriesRange to bt.RowRange
	expBase := string(Int64ToBytes(expiryMs / DefaultBucketSizeMilliSeconds))
	for i := range srs {
		if !s.hashLabels {
			return nil, errUnImpl
		}

		h128.Reset()
		h128.Write([]byte(srs[i].LabelsString))
		hashedLabels := string(h128.Sum(nil))
		seriesRangeBucket[hashedLabels] = srs[i].Labels

		if s.metricExpiration == 0 || !s.enableLongterm {
			// every thing belong to short term
			rrl = append(
				rrl,
				NewMetricRowRange(srs[i].Name, hashedLabels, srs[i].BaseStart, srs[i].BaseEnd),
			)
			continue
		}

		// check if this can go to long term
		if srs[i].BaseEnd < expBase {
			// only go to long term
			lrrl = append(
				lrrl,
				NewMetricRowRange(srs[i].Name, hashedLabels, srs[i].BaseStart, srs[i].BaseEnd),
			)
			continue
		}

		if srs[i].BaseStart > expBase {
			// only goes to shortterm
			rrl = append(
				rrl,
				NewMetricRowRange(srs[i].Name, hashedLabels, srs[i].BaseStart, srs[i].BaseEnd),
			)
			continue
		}

		// split
		rrl = append(
			rrl,
			NewMetricRowRange(srs[i].Name, hashedLabels, expBase, srs[i].BaseEnd),
		)
		lrrl = append(
			lrrl,
			NewMetricRowRange(srs[i].Name, hashedLabels, srs[i].BaseStart, expBase),
		)
	}

	var filters = []bigtable.Filter{
		bigtable.FamilyFilter(metricFamily),
	}

	filters = append(filters, bigtable.LatestNFilter(1), bigtable.StripValueFilter())

	var (
		readErr             error
		readFunc, lreadFunc func(r bigtable.Row) bool
	)

	makeReadFunc := func(startTs, endTs int64) func(r bigtable.Row) bool {
		return func(r bigtable.Row) bool {
			s.metrics.IncQueryMetricRowReadCount(1)
			// rowkey: <metric_name>#<base>hash(<labelsString>)
			//                     -> 8 bytes     ->16 bytes
			rk := []byte(r.Key())
			if len(rk) < hashedMetricRowKeySuffixLen {
				readErr = errors.New("invalid hashed metric row key read: '" + r.Key() + "'")
				return false
			}
			hashedLabelsString := string(rk[len(rk)-16-8 : len(rk)-8])
			// filter the hashed labels string is in the query
			if _, ok := seriesRangeBucket[hashedLabelsString]; !ok {
				return true
			}

			base := Int64FromBytes(rk[len(rk)-8:]) // [<name>#:<name>#<base>]
			ss := BtRowToPromSamples(base, r, startTs, endTs)

			if _, ok := sampleBuckets[hashedLabelsString]; !ok {
				// maintain order samples order
				if len(ss) != 0 {
					sampleLabelsStrings = append(sampleLabelsStrings, hashedLabelsString)
				}
			}
			sampleBuckets[hashedLabelsString] = append(sampleBuckets[hashedLabelsString], ss...)
			return true
		}
	}

	if !s.hashLabels {
		return nil, errUnImpl
	}

	if s.metricExpiration == 0 {
		// no expiration
		readFunc = makeReadFunc(qq.StartTimestampMs, qq.EndTimestampMs)
	} else {
		// with expiration, normal storage caps to expiration
		readFunc = makeReadFunc(expiryMs, qq.EndTimestampMs)

		if s.enableLongterm {
			// capped to short term
			lreadFunc = makeReadFunc(qq.StartTimestampMs, expiryMs) // read one ms less
		}
	}

	if s.metricExpiration != 0 && s.enableLongterm && len(lrrl) > 0 {
		rset := btrowset.RowSet{
			RowRanges: lrrl,
		}

		err = s.ltbl.ReadRows(ctx, rset, lreadFunc, bigtable.RowFilter(
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
	}

	if len(rrl) > 0 {
		rset := btrowset.RowSet{
			RowRanges: rrl,
		}

		err = s.tbl.ReadRows(ctx, rset, readFunc, bigtable.RowFilter(
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
	}

	var res = make([]*prompb.TimeSeries, len(sampleLabelsStrings))
	for i := range sampleLabelsStrings {
		if _, ok := seriesRangeBucket[sampleLabelsStrings[i]]; !ok {
			fmt.Printf("\nlabels string is not found %s\n", sampleLabelsStrings[i])
		}
		ts := &prompb.TimeSeries{
			Labels:  append([]prompb.Label{{Name: MetricNameLabel, Value: name}}, seriesRangeBucket[sampleLabelsStrings[i]]...),
			Samples: sampleBuckets[sampleLabelsStrings[i]],
		}
		// sort samples
		// sort.Slice(ts.Samples, func(i int, j int) bool {
		// 	return ts.Samples[i].Timestamp < ts.Samples[j].Timestamp
		// })
		res[i] = ts
		s.metrics.IncQueryMetricSampleReadCount(len(ts.Samples))
		// i++
	}

	return res, nil
}

// RunMetaCacheGC -
func (s *Store) RunMetaCacheGC() {
	s.logger.Info("RunMetaCacheGC started")
	// setup ticker for every 10min
	c := time.NewTicker(10 * time.Minute)
	defer c.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("RunMetaCacheGC exist")
			return
		case <-c.C:
			t := time.Now().Add(-10 * time.Minute)
			s.metaWriteCache.Range(func(k, v interface{}) bool {
				if v.(time.Time).Before(t) {
					// 10 min old
					s.metaWriteCache.Delete(k)
					s.logger.Info("RunMetaCacheGC deletes", zap.String("k", k.(string)))
				}
				return true
			})
		}
	}
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

// String -
func (s *SeriesRange) String() string {
	return fmt.Sprintf(
		"SeriesRange: [StartMs: %d, EndMs: %d, BaseStart: %x, BaseEnd: %x, Name: %s, LabelsString: %s, Labels: %v",
		s.StartMs, s.EndMs, s.BaseStart, s.BaseEnd, s.Name, s.LabelsString, s.Labels,
	)
}

// NewMetricRowRange - generate bigtable.RowRange
func NewMetricRowRange(name, hashedLabelsString, baseStart, baseEnd string) *btpb.RowRange {
	var res = new(btpb.RowRange)
	if baseStart == "" {
		baseStart = zeroBaseStart
	}
	if baseEnd == "" {
		baseEnd = maxBaseEnd
	}

	res.StartKey = btrowset.RowKey([]byte(name + "#" + hashedLabelsString + baseStart)).KeyRangeStartClosed()
	if baseStart == baseEnd {
		res.EndKey = btrowset.RowKey([]byte(name + "#" + hashedLabelsString + prefixSuccessor(baseEnd))).KeyRangeEndClosed()
	} else {
		res.EndKey = btrowset.RowKey([]byte(name + "#" + hashedLabelsString + baseEnd)).KeyRangeEndClosed()
	}

	return res
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
		rs          = btrowset.RowSet{
			RowRanges: []*btpb.RowRange{rr},
			// RowRanges: []*btpb.RowRange{{}},
		}
		metaRows []metaRowBaseWithColumn

		filters = []bigtable.Filter{
			bigtable.FamilyFilter(indexRowLabelFamily),
			bigtable.LatestNFilter(1),
			bigtable.StripValueFilter(),
		}
	)
	err = s.mtbl.ReadRows(ctx, rs, func(r bigtable.Row) bool {
		rk := r.Key()
		if len(rk) != rkLen {
			readErr = errors.New("invalid rowkey length '" + rk + "'")
			return false
		}
		baseStr := rk[rkPrefixLen:]

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
	// convert metaRows into SeriesRange and filter out
	// which metaRowBaseWithColumn are actually appliable to the query.
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
	s.metrics.IncQueryMetaColumnReadCount(true, len(matchedLabels))
	s.metrics.IncQueryMetaColumnReadCount(false, len(metaRows)-len(matchedLabels))
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

// // DropMetricsOption -
// type DropMetricsOption struct {
// 	Before time.Time
// }

// // DropMetrics -
// func (s *Store) DropMetrics(ctx context.Context, opt DropMetricsOption) error {
// 	if opt.Before.IsZero() {
// 		return errors.New("drop metrics before is required")
// 	}

// }

// QueryToBigtableMetaRowRange -
func QueryToBigtableMetaRowRange(name string, startTs, endTs int64) (*btpb.RowRange, error) {
	prefix := name + "#"
	if endTs == 0 {
		endTs = math.MaxInt64
	}
	var res = new(btpb.RowRange)
	res.StartKey = btrowset.NewRowKey([]byte(prefix + string(Int64ToBytes(startTs/DefaultBucketSizeMilliSeconds)))).KeyRangeStartClosed()
	res.EndKey = btrowset.NewRowKey([]byte(prefix + string(Int64ToBytes(endTs/DefaultBucketSizeMilliSeconds)))).KeyRangeEndClosed()
	return res, nil
}

func (s *Store) createTableIfNotExist(ctx context.Context, adc *bigtable.AdminClient, table string) error {
	_, err := adc.TableInfo(ctx, table)
	if err != nil {
		if grpc.Code(err) != codes.NotFound {
			return err
		}
		// create table.
		if err2 := adc.CreateTable(ctx, table); err2 != nil {
			return err2
		}
	}
	return nil
}

func (s *Store) createColumnFamilyIfNotExist(ctx context.Context, adc *bigtable.AdminClient, table string, family string) error {
	if err := adc.CreateColumnFamily(ctx, table, family); err != nil {
		if grpc.Code(err) != codes.AlreadyExists {
			return err
		}
		return nil
	}
	return nil
}

// QueryToBigtableTimeFilter -
func QueryToBigtableTimeFilter(startMs, endMs int64) bigtable.Filter {
	if startMs == 0 && endMs == 0 {
		return nil
	}
	// bt TimestampRange, start is inclusive, end is exclusive
	if endMs == 0 {
		return bigtable.TimestampRangeFilterMicros(bigtable.Timestamp(startMs*1e3), bigtable.Timestamp(0))
	}
	return bigtable.TimestampRangeFilterMicros(bigtable.Timestamp(startMs*1e3), bigtable.Timestamp((endMs+1)*1e3))
}

// TimestampToColumn column is the delta within the range.
func TimestampToColumn(ts int64) string {
	buf := make([]byte, 4)
	n := binary.PutUvarint(buf, uint64(ts%DefaultBucketSizeMilliSeconds))
	return string(buf[:n])
}

// BtRowToPromSamples -
func BtRowToPromSamples(base int64, r bigtable.Row, startTs, endTs int64) []prompb.Sample {
	// var samples = make([]prompb.Sample, len(r[metricFamily]))
	var samples []prompb.Sample
	if endTs <= 0 {
		endTs = math.MaxInt64
	}
	// fill out samples
	for _, v := range r[metricFamily] {
		// make sure column is valid.
		if len(v.Column) != fullColLen {
			// panic(fmt.Errorf("column invalid %X", v.Column))
			continue // TODO: log
		}
		colBytes := []byte(v.Column)
		remainder := Int32FromBytes(colBytes[metricFamilyPrefixLen : metricFamilyPrefixLen+4])
		ts := base*DefaultBucketSizeMilliSeconds + int64(remainder)
		if ts < startTs || ts > endTs {
			continue
		}
		value := Float64FromBytes(colBytes[metricFamilyPrefixLen+4:])
		samples = append(samples, prompb.Sample{
			Value:     value,
			Timestamp: ts,
		})
	}
	// have to sort due to column is uvarint now... sort order is not maintained.
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Timestamp < samples[j].Timestamp
	})
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

// TimeMs -
func TimeMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}
