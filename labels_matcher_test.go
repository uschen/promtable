package promtable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
)

func TestQueryMatchersToLabelsMatcher(t *testing.T) {
	tests := []struct {
		input      []*prompb.LabelMatcher
		matches    [][]prompb.Label
		notmatches [][]prompb.Label
	}{
		{
			input: []*prompb.LabelMatcher{
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
			},
			matches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2#v2.1"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2#v2.1"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2#v2.1"}, {Name: "l3", Value: "v3"}}, // more values
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2#v2.1"}},
			},
			notmatches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}}, // one wrong value
				[]prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2"}},      // two wrong values
				[]prompb.Label{},
				[]prompb.Label{{Name: "l2", Value: "v2#v2.1"}, {Name: "l1", Value: "v1,v1.2"}},
			},
		},
		{
			input: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "l2",
					Value: "pods",
				},
			},
			matches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "pods"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "pods"}, {Name: "l3", Value: "v3"}}, // more values
			},
			notmatches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}}, // one wrong value
				[]prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2"}},      // two wrong values
				[]prompb.Label{},
			},
		},
		{
			input: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "l2",
					Value: "pods",
				},
			},
			matches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}},
				[]prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2"}},
				[]prompb.Label{},
			},
			notmatches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "pods"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "pods"}, {Name: "l3", Value: "v3"}}, // more values
			},
		},
		{
			input: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "l2",
					Value: "v2",
				},
			},
			matches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}, {Name: "l3", Value: "v3"}}, // more values
			},
			notmatches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v3"}}, // one wrong value
				[]prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v4"}},      // two wrong values
				[]prompb.Label{},
			},
		},
		{
			input: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "l2",
					Value: "v2",
				},
			},
			matches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v3"}},
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v3"}, {Name: "l3", Value: "v3"}}, // more values
				[]prompb.Label{},
			},
			notmatches: [][]prompb.Label{
				[]prompb.Label{{Name: "l1", Value: "v1,v1.2"}, {Name: "l2", Value: "v2"}}, // one wrong value
				[]prompb.Label{{Name: "l1", Value: "v1"}, {Name: "l2", Value: "v2"}},      // two wrong values
			},
		},
	}
	for _, test := range tests {
		actual, err := promtable.QueryMatchersToLabelsMatcher(test.input)
		assert.Nil(t, err)
		for i := range test.matches {
			// sort.Slice(test.matches[i], func(k, j int) bool { return test.matches[i][k].Name < test.matches[i][j].Name })
			ok := actual.Match(test.matches[i])
			assert.True(t, ok, "should match: ", test.matches[i])
		}
		for i := range test.notmatches {
			// sort.Slice(test.notmatches[i], func(k, j int) bool { return test.notmatches[i][k].Name < test.notmatches[i][j].Name })
			ok := actual.Match(test.notmatches[i])
			assert.False(t, ok, "should NOT match: ", test.notmatches[i])
		}
	}
}
