package promtable_test

import (
	"bytes"
	"log"
	"sort"
	"testing"

	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
	"github.com/uschen/promtable/util/testutil"
)

func TestLabelsToMetric(t *testing.T) {
	cases := []struct {
		note   string
		input  []prompb.Label
		output *promtable.Metric
	}{
		{
			note: "pick out MetricNameLabel",
			input: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l2", Value: "v2,v21"},
				{Name: "l1", Value: "v1"},
			},
			output: &promtable.Metric{
				Name:   "ma",
				Labels: []prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2,v21"}},
			},
		},
		{
			note: "no MetricNameLabel",
			input: []prompb.Label{
				{Name: "l2", Value: "v2"},
				{Name: "l1", Value: "v1"},
			},
			output: &promtable.Metric{
				Labels: []prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2"}},
			},
		},
	}

	for _, cas := range cases {
		actual := promtable.LabelsToMetric(cas.input)
		testutil.Equals(t, cas.output, actual)
	}
}

func TestEscapeLabelValue(t *testing.T) {
	cases := []struct {
		note   string
		input  string
		output string
	}{
		{
			note:   "# -> %23, , -> %2C",
			input:  "a#b_is_,c",
			output: "a%23b_is_%2cc",
		},
		{
			note:   "empty",
			input:  "",
			output: "",
		},
	}
	for _, cas := range cases {
		actual := promtable.EscapeLabelValue(cas.input)
		testutil.Equals(t, cas.output, actual)
	}
}

func TestUnescapeLabelValue(t *testing.T) {
	cases := []struct {
		note   string
		input  string
		output string
	}{
		{
			note:   " %23 -> #, %2C -> ,",
			input:  "a%23b_is_,%2cc",
			output: "a#b_is_,,c",
		},
	}
	for _, cas := range cases {
		actual := promtable.UnescapeLabelValue(cas.input)
		testutil.Equals(t, cas.output, actual)
	}
}

func TestMetric_LabelsString(t *testing.T) {
	cases := []struct {
		input  *promtable.Metric
		output string
	}{
		{
			input: &promtable.Metric{
				Name:   "ma",
				Labels: []prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2,21"}},
			},
			output: "l1=v1,l2=v2%2c21",
		},
	}

	for _, cas := range cases {
		actual := cas.input.LabelsRowString()
		testutil.Equals(t, cas.output, actual)
	}
}

func TestInt64ToBytes(t *testing.T) {
	cases := []struct {
		input  int64
		output []byte
	}{
		{
			input:  1,
			output: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
		},
		{
			input:  12,
			output: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00},
		},
	}

	for _, cas := range cases {
		actual := promtable.Int64ToBytes(cas.input)
		testutil.Equals(t, cas.output, actual)
	}

	var res sortByteArrays
	for i := int64(0); i < 100000000; i += 100 {
		res = append(res, promtable.Int64ToBytes(i))
	}
	testutil.Equals(t, sort.IsSorted(res), true)
}

// implement `Interface` in sort package.
type sortByteArrays [][]byte

func (b sortByteArrays) Len() int {
	return len(b)
}

func (b sortByteArrays) Less(i, j int) bool {
	// bytes package already implements Comparable for []byte.
	switch bytes.Compare(b[i], b[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		log.Panic("not fail-able with `bytes.Comparable` bounded [-1, 1].")
		return false
	}
}

func (b sortByteArrays) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func TestRowsFromTimeseries(t *testing.T) {
	cases := []struct {
		namespace string
		input     prompb.TimeSeries
		output    []*promtable.TimeseriesRow
	}{
		{
			namespace: "ns",
			input: prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: promtable.MetricNameLabel, Value: "ma"},
					{Name: "l2", Value: "v2"},
					{Name: "l1", Value: "v1"},
				},
				Samples: []prompb.Sample{
					{Value: 234.56, Timestamp: 2*60*60*1000 + 123},
					{Value: 123.45, Timestamp: 60*60*1000 + 123},
					{Value: 234.56, Timestamp: 60*60*1000 + 234},
				},
			},
			output: []*promtable.TimeseriesRow{
				{
					RowKey: "ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l1=v1,cl2=v2",
					Values: map[int64][]uint8{
						7200123: []uint8{0x40, 0x6d, 0x51, 0xeb, 0x85, 0x1e, 0xb8, 0x52, 0x0, 0x0},
					},
				},
				{
					RowKey: "ns#ma#\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00#l1=v1,cl2=v2",
					Values: map[int64][]uint8{
						3600123: []uint8{0x40, 0x5e, 0xdc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcd, 0x0, 0x0},
						3600234: []uint8{0x40, 0x6d, 0x51, 0xeb, 0x85, 0x1e, 0xb8, 0x52, 0x0, 0x0},
					},
				},
			},
		},
	}

	for _, cas := range cases {
		actual, err := promtable.RowsFromTimeseries(cas.namespace, cas.input)
		testutil.Ok(t, err)

		testutil.Equals(t, len(actual), len(cas.output))
		for i, r := range actual {
			testutil.Equals(t, cas.output[i].Values, r.Values)
		}
	}
}
