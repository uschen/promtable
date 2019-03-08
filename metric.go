package promtable

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricNameQuerySeriesRangesCount     = "promtable_query_series_ranges_count"
	metricNameQueryMetaColumnReadCount   = "promtable_query_meta_column_read_count"
	metricNameQueryMetricRowReadCount    = "promtable_query_metric_row_read_count"
	metricNameQueryMetricSampleReadCount = "promtable_query_metric_sample_read_count"
	metricNameQueryTimeseriesReadCount   = "promtable_query_timeseries_read_count"
	metricNamePutTimeseriesCount         = "promtable_put_timeseries_count"
	metricNamePutSampleCount             = "promtable_put_sample_count"
)

// StoreMetrics -
type StoreMetrics struct {
	querySeriesRangesCount         prometheus.Counter
	queryMetaColumnReadCount       *prometheus.CounterVec
	queryMetricRowReadCount        prometheus.Counter
	queryMetricSampleReadCount     prometheus.Counter
	queryMetricTimeseriesReadCount prometheus.Counter
	putTimeseriesCount             prometheus.Counter
	putSampleCount                 prometheus.Counter
}

// NewStoreMetrics -
func NewStoreMetrics() *StoreMetrics {
	m := &StoreMetrics{
		querySeriesRangesCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNameQuerySeriesRangesCount,
				Help: "query meta # of returned SeriesRanges",
			},
		),
		queryMetaColumnReadCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: metricNameQueryMetaColumnReadCount,
				Help: "query meta # bigtable column read",
			},
			[]string{"matched"},
		),
		queryMetricRowReadCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNameQueryMetricRowReadCount,
				Help: "query meta # row read",
			},
		),
		queryMetricSampleReadCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNameQueryMetricSampleReadCount,
				Help: "query # of returned samples",
			},
		),
		queryMetricTimeseriesReadCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNameQueryTimeseriesReadCount,
				Help: "query # of returned timeseries",
			},
		),
		putTimeseriesCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNamePutTimeseriesCount,
				Help: "put # timeseries",
			},
		),
		putSampleCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: metricNamePutSampleCount,
				Help: "put # samples",
			},
		),
	}

	prometheus.MustRegister(m.querySeriesRangesCount)
	prometheus.MustRegister(m.queryMetaColumnReadCount)
	prometheus.MustRegister(m.queryMetricRowReadCount)
	prometheus.MustRegister(m.queryMetricSampleReadCount)
	prometheus.MustRegister(m.queryMetricTimeseriesReadCount)
	prometheus.MustRegister(m.putTimeseriesCount)
	prometheus.MustRegister(m.putSampleCount)

	return m
}

// IncQuerySeriesRangesCount -
func (m *StoreMetrics) IncQuerySeriesRangesCount(delta int) {
	if m == nil {
		return
	}
	m.querySeriesRangesCount.Add(float64(delta))
}

// IncQueryMetaColumnReadCount -
func (m *StoreMetrics) IncQueryMetaColumnReadCount(matched bool, delta int) {
	if m == nil {
		return
	}
	m.queryMetaColumnReadCount.WithLabelValues(strconv.FormatBool(matched)).Add(float64(delta))
}

// IncQueryMetricSampleReadCount -
func (m *StoreMetrics) IncQueryMetricSampleReadCount(delta int) {
	if m == nil {
		return
	}
	m.queryMetricSampleReadCount.Add(float64(delta))
}

// IncQueryMetricRowReadCount -
func (m *StoreMetrics) IncQueryMetricRowReadCount(delta int) {
	if m == nil {
		return
	}
	m.queryMetricRowReadCount.Add(float64(delta))
}

// IncQueryMetricTimeseriesReadCount -
func (m *StoreMetrics) IncQueryMetricTimeseriesReadCount(delta int) {
	if m == nil {
		return
	}
	m.queryMetricTimeseriesReadCount.Add(float64(delta))
}

// IncPutTimeseriesCount -
func (m *StoreMetrics) IncPutTimeseriesCount(delta int) {
	if m == nil {
		return
	}
	m.putTimeseriesCount.Add(float64(delta))
}

// IncPutSampleCount -
func (m *StoreMetrics) IncPutSampleCount(delta int) {
	if m == nil {
		return
	}
	m.putSampleCount.Add(float64(delta))
}
