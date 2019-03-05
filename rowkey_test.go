package promtable_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
)

func TestLabelsToRowKeyComponents(t *testing.T) {
	tests := []struct {
		input           []prompb.Label
		expName         string
		expLabelsString string
		expErr          error
	}{
		{
			input: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
			},
			expName:         "ma",
			expLabelsString: "l1,l2,v1,v2",
		},
		{
			input: []prompb.Label{
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
			},
			expErr: errors.New(""),
		},
		{
			input: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2,3"},
			},
			expName:         "ma",
			expLabelsString: "l1,l2,v1,v2%2c3",
		},
		{
			input: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l1", Value: ""},
				{Name: "l2", Value: "v2"},
			},
			expName:         "ma",
			expLabelsString: "l1,l2,,v2",
		},
	}

	for _, test := range tests {
		name, labelsString, err := promtable.LabelsToRowKeyComponents(test.input)
		if test.expErr == nil {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
		assert.Equal(t, test.expName, name)
		assert.Equal(t, test.expLabelsString, labelsString)
	}
}

func TestLabelsFromString(t *testing.T) {
	tests := []struct {
		input     string
		expLabels []prompb.Label
		expErr    error
	}{
		{
			input: "l1,l2,l3,l4,l5,,v2,v3,v4,v5%2c6",
			expLabels: []prompb.Label{
				{Name: "l1", Value: ""},
				{Name: "l2", Value: "v2"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4"},
				{Name: "l5", Value: "v5,6"},
			},
		},
		{
			input: "",
		},
		{
			input:  "l1,l2,v1",
			expErr: errors.New(""),
		},
	}

	for _, test := range tests {
		labels, err := promtable.LabelsFromString(test.input)
		if test.expErr == nil {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
		assert.EqualValues(t, test.expLabels, labels)
	}
}
