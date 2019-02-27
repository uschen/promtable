package promtable

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/uschen/promtable/prompb"
)

const (
	hourInMs = 60 * 60 * 1000
)

// Labels -
type Labels []prompb.Label

func (l Labels) Len() int           { return len(l) }
func (l Labels) Less(i, j int) bool { return l[i].Name < l[j].Name }
func (l Labels) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// Metric -
type Metric struct {
	Name   string
	Labels Labels
	// LabelStrings []UnescapedLabelString
}

// LabelsRowString -
func (m *Metric) LabelsRowString() string {
	var out strings.Builder
	for i := range m.Labels {
		out.WriteString(m.Labels[i].Name)
		out.WriteByte('=')
		out.WriteString(EscapeLabelValue(m.Labels[i].Value))
		if i != len(m.Labels)-1 {
			out.WriteByte(',')
		}
	}
	return out.String()
}

// RowKey -
type RowKey struct{}

// TimeseriesRow -
type TimeseriesRow struct {
	RowKey string
	Values map[int64][]byte // map[ts][]byte
}

// RowsFromTimeseries -
// per-hour-bucket
func RowsFromTimeseries(namespace string, ts prompb.TimeSeries) ([]*TimeseriesRow, error) {
	metric := LabelsToMetric(ts.Labels)
	if metric.Name == "" {
		return nil, errors.New("no name metric")
	}

	// separate samples into base value buckets
	// map[hours]map[ts_ms]value
	var buckets = make(map[int64]map[int64]float64)

	for i := range ts.Samples {
		base := ts.Samples[i].Timestamp / hourInMs
		if buckets[base] == nil {
			buckets[base] = make(map[int64]float64)
		}
		buckets[base][ts.Samples[i].Timestamp] = ts.Samples[i].Value
	}

	// build row
	rkPrefix := namespace + "#" + string(metric.Name) + "#"
	labelsString := metric.LabelsRowString()
	var rows []*TimeseriesRow
	for k, v := range buckets {
		r := new(TimeseriesRow)
		r.Values = make(map[int64][]byte)
		var rkB strings.Builder
		rkB.WriteString(rkPrefix)
		rkB.Write(Int64ToBytes(k)) // write base
		rkB.WriteByte('#')
		rkB.WriteString(string(labelsString))
		r.RowKey = rkB.String()

		for ts, value := range v {
			r.Values[ts] = Float64ToBytes(value)
		}
		rows = append(rows, r)
	}

	return rows, nil
}

// LabelsToMetric __name__ will be used for Metric name
func LabelsToMetric(labels []prompb.Label) (m *Metric) {
	m = new(Metric)
	for i := range labels {
		if labels[i].Name != MetricNameLabel {
			m.Labels = append(m.Labels, labels[i])
		} else {
			m.Name = labels[i].Value
		}
	}
	sort.Sort(m.Labels)
	return m
}

func timestampToBaseAndOffset(ts int64) (base, offset int64) {
	return ts / hourInMs, ts % hourInMs
}

// Float64ToBytes -
func Float64ToBytes(val float64) []byte {
	typeBuf := make([]byte, binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(typeBuf[:], math.Float64bits(val))
	return typeBuf
}

// Float64FromBytes -
func Float64FromBytes(val []byte) float64 {
	if len(val) == 0 {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(val))
}

// Int64ToBytes -
func Int64ToBytes(val int64) []byte {
	typeBuf := make([]byte, binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(typeBuf, uint64(val))
	return typeBuf
}

// Int64FromBytes -
func Int64FromBytes(val []byte) int64 {
	if len(val) == 0 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val))
}

var escapeReplacer = strings.NewReplacer(
	"#", "%23",
	",", "%2c",
)

var unescapeReplacer = strings.NewReplacer(
	"%23", "#",
	"%2c", ",",
)

// EscapeLabelValue -
// # -> %23
// , -> %2C
func EscapeLabelValue(v string) string {
	return escapeReplacer.Replace(v)
}

// UnescapeLabelValue -
func UnescapeLabelValue(v string) string {
	return unescapeReplacer.Replace(v)
}
