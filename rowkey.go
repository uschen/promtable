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
	// write label.Name first
	for i := 1; i < len(labels); i++ {
		lsb.WriteString(labels[i].Name)
		lsb.WriteRune(',')
	}
	for i := 1; i < len(labels)-1; i++ {
		lsb.WriteString(EscapeLabelValue(labels[i].Value))
		lsb.WriteRune(',')
	}
	lsb.WriteString(EscapeLabelValue(labels[len(labels)-1].Value))

	// for i := 1; i < len(labels); i++ {
	// 	lsb.WriteString(labels[i].Name)
	// 	lsb.WriteString("=")
	// 	lsb.WriteString(EscapeLabelValue(labels[i].Value))
	// 	if i != len(labels)-1 {
	// 		lsb.WriteString(",")
	// 	}
	// }
	return name, lsb.String(), nil
}

// LabelsFromString -
func LabelsFromString(labelsString string) ([]prompb.Label, error) {
	if labelsString == "" {
		return nil, nil
	}
	parts := strings.Split(labelsString, ",")
	var (
		n   = len(parts)
		n12 = n / 2
	)

	if n%2 != 0 {
		return nil, errors.New("invalid labelsString received '" + labelsString + "'")
	}
	var labels = make([]prompb.Label, n12)
	for i := 0; i < n12; i++ {
		labels[i] = prompb.Label{
			Name:  parts[i],
			Value: UnescapeLabelValue(parts[i+n12]),
		}
	}
	return labels, nil
}
