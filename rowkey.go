package promtable

import (
	"errors"
	"strings"

	"github.com/uschen/promtable/prompb"
)

// LabelsToRowKeyComponents - labels must be sorted
func LabelsToRowKeyComponents(labels []prompb.Label) (name, labelsString string, err error) {
	if len(labels) == 0 || labels[0].Name != MetricNameLabel {
		err = errors.New("no metric name in labels")
		return
	}
	name = labels[0].Value
	var lsb strings.Builder
	for i := 1; i < len(labels); i++ {
		lsb.WriteString(labels[i].Name)
		lsb.WriteString("=")
		lsb.WriteString(EscapeLabelValue(labels[i].Value))
		if i != len(labels)-1 {
			lsb.WriteString(",")
		}
	}
	return name, lsb.String(), nil
}

// LabelsFromString -
func LabelsFromString(labelsString string) ([]prompb.Label, error) {
	labelStrings := strings.Split(labelsString, ",")
	if len(labelStrings) == 0 {
		return nil, nil
	}
	var labels = make([]prompb.Label, len(labelStrings))
	for i := range labelStrings {
		kv := strings.Split(labelStrings[i], "=")
		if len(kv) != 2 {
			return nil, errors.New("unable to parse label: '" + labelStrings[i] + "'")
		}
		labels[i] = prompb.Label{}
		labels[i].Name, labels[i].Value = kv[0], UnescapeLabelValue(kv[1])
	}
	return labels, nil
}
