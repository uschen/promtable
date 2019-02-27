package promtable_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
	"github.com/uschen/promtable/util/testutil"
)

func TestQueryToBigtableRowRange(t *testing.T) {
	cases := []struct {
		note      string
		namespace string
		input     *prompb.Query
		notOK     bool
		beginEnd  string
	}{
		{
			note:      "no metric name",
			namespace: "ns",
			input: &prompb.Query{
				StartTimestampMs: 0,
				EndTimestampMs:   0,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l2",
						Value: "v2",
					},
				},
			},
			notOK: true,
		},
		{
			note:      "metric name Type is not EQ",
			namespace: "ns",
			input: &prompb.Query{
				StartTimestampMs: 0,
				EndTimestampMs:   0,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1",
					},
					{
						Type:  prompb.LabelMatcher_NEQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
				},
			},
			notOK: true,
		},
		{
			note:      "start ms and end ms are 0",
			namespace: "ns",
			input: &prompb.Query{
				StartTimestampMs: 0,
				EndTimestampMs:   0,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
				},
			},
			beginEnd: "[\"ns#ma#\",\"ns#ma$\")",
		},
		{
			note:      "end ms is 0, should prefixSuccessor",
			namespace: "ns",
			input: &prompb.Query{
				StartTimestampMs: 60*60*1000 + 123,
				EndTimestampMs:   0,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
				},
			},
			beginEnd: "[\"ns#ma#\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00#\",\"ns#ma$\")",
		},
		{
			note:      "begin and end are not 0",
			namespace: "ns",
			input: &prompb.Query{
				StartTimestampMs: 60*60*1000 + 123,
				EndTimestampMs:   2*60*60*1000 + 123,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
				},
			},
			beginEnd: "[\"ns#ma#\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00#\",\"ns#ma#\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03\\x00\\x00#\")",
		},
	}
	for _, cas := range cases {
		actual, ok := promtable.QueryToBigtableRowRange(cas.namespace, cas.input)
		if cas.notOK {
			testutil.Equals(t, false, ok)
		} else {
			testutil.Equals(t, true, ok)
			testutil.Equals(t, actual == nil, false)
			rr, ok1 := actual.(bigtable.RowRange)
			testutil.Equals(t, true, ok1)
			testutil.Equals(t, cas.beginEnd, rr.String())
		}
	}
}

func TestLabelMatcherToFitlerFunc(t *testing.T) {
	tests := []struct {
		note       string
		input      *prompb.LabelMatcher
		matches    []prompb.Label
		notmatches []prompb.Label
	}{
		{
			note: "support EQ",
			input: &prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "l1",
				Value: "v1",
			},
			matches:    []prompb.Label{{Name: "l1", Value: "v1"}},
			notmatches: []prompb.Label{{Name: "l1", Value: "v2"}, {Name: "l2", Value: "v1"}},
		},
		{
			note: "support NEQ",
			input: &prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_NEQ,
				Name:  "l1",
				Value: "v1",
			},
			matches:    []prompb.Label{{Name: "l1", Value: "v2"}, {Name: "l2", Value: "v1"}},
			notmatches: []prompb.Label{{Name: "l1", Value: "v1"}},
		},
		{
			note: "support RE",
			input: &prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_RE,
				Name:  "l1",
				Value: "v1|v2|v3",
			},
			matches:    []prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l1", Value: "v2"}, {Name: "l1", Value: "v3"}},
			notmatches: []prompb.Label{{Name: "l2", Value: "v1"}, {Name: "l1", Value: "v4"}},
		},
		{
			note: "support NRE",
			input: &prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_NRE,
				Name:  "l1",
				Value: "v1|v2|v3",
			},
			matches:    []prompb.Label{{Name: "l2", Value: "v1"}, {Name: "l1", Value: "v4"}},
			notmatches: []prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l1", Value: "v2"}, {Name: "l1", Value: "v3"}},
		},
	}
	for _, test := range tests {
		actual := promtable.LabelMatcherToFitlerFunc(test.input)
		for i := range test.matches {
			// t.Logf("\ni: %d\n", i)
			testutil.Equals(t, true, actual(test.matches[i]))
		}
		for i := range test.notmatches {
			// t.Logf("\ni: %d\n", i)
			testutil.Equals(t, false, actual(test.notmatches[i]))
		}
	}
}

func TestQueryToRowKeyFilter(t *testing.T) {
	tests := []struct {
		note       string
		input      *prompb.Query
		matches    []string
		notmatches []string
	}{
		{
			note: "will filter based on label key and values",
			input: &prompb.Query{
				StartTimestampMs: 60*60*1000 + 123,
				EndTimestampMs:   2*60*60*1000 + 123,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l1",
						Value: "v1,v1.2",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l2",
						Value: "v2#v2.1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
				},
			},
			matches: []string{
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l1=v1%2cv1.2,l2=v2%23v2.1",
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l2=v2%23v2.1,l1=v1%2cv1.2",
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l2=v2%23v2.1,l1=v1%2cv1.2,l3=v3", // more values
				"ns#ma##l2=v2%23v2.1,l1=v1%2cv1.2",
			},
			notmatches: []string{
				"ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l2=v2%23v2.1,l1=v1%2cv1.2", // missing namespace
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l2=v2,l1=v1%2cv1.2",     // one wrong value
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#l2=v2,l1=v1",            // two wrong values
				"ns#ma#\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00#",                       // no values
			},
		},
	}

	for _, test := range tests {
		actual := promtable.QueryToRowKeyFilter(test.input)
		for i := range test.matches {
			name, _, _, ok := actual(test.matches[i])
			testutil.Equals(t, true, ok)
			testutil.Equals(t, "ma", name)
		}
		for i := range test.notmatches {
			_, _, _, ok := actual(test.notmatches[i])
			testutil.Equals(t, false, ok)
		}
	}
}

func TestParseRowKey(t *testing.T) {
	rk := "ns#ma#\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00#l1=v1,l2=v2,l3=v3,l4=v4%2c%234"
	name, _, labelStrings, ok := promtable.ParseRowKey(rk)
	testutil.Equals(t, "ma", name)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, 4, len(labelStrings))
}
