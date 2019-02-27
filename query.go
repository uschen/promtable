package promtable

import (
	"regexp"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"

	"github.com/uschen/promtable/prompb"
)

// LabelStrings -
type LabelStrings []string

// MetricIdentifier can be used to uniquely identify a metric
// format: <name>#labelsString
type MetricIdentifier string

// RowKeyFilterFunc -
type RowKeyFilterFunc func(rk string) (name string, metricID MetricIdentifier, labels Labels, ok bool)

// QueryToRowKeyFilter will generate a filter function that check
// if the given rowkey should be returned for the Query.
// will not filter namespace, name and timestamp.
func QueryToRowKeyFilter(q *prompb.Query) RowKeyFilterFunc {
	var matcherFuncs = make([]LabelFilterFunc, len(q.Matchers))
	for i := range q.Matchers {
		if q.Matchers[i].Name == MetricNameLabel {
			continue
		}
		matcherFuncs[i] = LabelMatcherToFitlerFunc(q.Matchers[i])
	}
	return func(rk string) (name string, metricID MetricIdentifier, labels Labels, ok bool) {
		// row key is <ns>#<name>#<base>#<labels>
		name, metricID, labels, ok = ParseRowKey(rk)
		if !ok {
			return "", "", nil, false
		}

		var pendingMatches = len(matcherFuncs)

	MatchersLoop:
		for i := range matcherFuncs {
			if matcherFuncs[i] == nil {
				pendingMatches--
				continue MatchersLoop
			}
			for k := range labels {
				if matcherFuncs[i](labels[k]) {
					pendingMatches--
					continue MatchersLoop
				}
			}
		}

		if pendingMatches != 0 {
			return name, metricID, labels, false
		}
		return name, metricID, labels, true
	}
}

// LabelFilterFunc -
type LabelFilterFunc func(lable prompb.Label) bool

// LabelMatcherToFitlerFunc -
func LabelMatcherToFitlerFunc(lm *prompb.LabelMatcher) LabelFilterFunc {

	var (
		not bool

		ln = lm.Name
		lv = lm.Value
	)
	switch lm.Type {
	case prompb.LabelMatcher_NEQ:
		not = true
		fallthrough
	case prompb.LabelMatcher_EQ:
		trueFunc := func(label prompb.Label) bool {
			return label.Name == ln && label.Value == lv
		}

		if not {
			return func(label prompb.Label) bool {
				return !trueFunc(label)
			}
		}
		return trueFunc
	case prompb.LabelMatcher_NRE:
		not = true
		fallthrough
	case prompb.LabelMatcher_RE:
		regexMatch, err := regexp.Compile(lv)
		if err != nil {
			return func(label prompb.Label) bool { return false }
		}
		trueFunc := func(label prompb.Label) bool {
			if label.Name != lm.Name {
				return false
			}
			return regexMatch.MatchString(label.Value)
		}
		if !not {
			return trueFunc
		}

		return func(label prompb.Label) bool {
			return !trueFunc(label)
		}
	}
	return func(prompb.Label) bool { return false }
}

// QueryToBigtableRowRange - based on namespace, name, begin_time, end_time to generate RowRange
func QueryToBigtableRowRange(namespace string, q *prompb.Query) (rr bigtable.RowSet, ok bool) {
	var name string
	// find metric name
	for i := range q.Matchers {
		if q.Matchers[i].Name == MetricNameLabel {
			if q.Matchers[i].Type == prompb.LabelMatcher_EQ {
				// can ONLY do metric name EQ match.
				name = q.Matchers[i].Value
				break
			}
		}
	}
	if name == "" {
		return rr, false
	}
	prefix := namespace + "#" + name + "#"
	if q.StartTimestampMs == 0 && q.EndTimestampMs == 0 {
		rr = bigtable.PrefixRange(prefix)
		return rr, true
	}
	var (
		begin, end string
	)
	var beginBuilder strings.Builder
	beginBuilder.WriteString(prefix)
	beginBuilder.Write(Int64ToBytes(q.StartTimestampMs / hourInMs))
	beginBuilder.WriteByte('#')
	begin = beginBuilder.String()
	if q.EndTimestampMs == 0 {
		end = prefixSuccessor(prefix)
	} else {
		end = prefix + string(Int64ToBytes(q.EndTimestampMs/hourInMs+1)) + "#"
	}
	rr = bigtable.NewRange(begin, end)
	return rr, true
}

// QueryToCellTimestampFilter -
func QueryToCellTimestampFilter(q *prompb.Query) bigtable.Filter {
	if q.StartTimestampMs == 0 && q.EndTimestampMs == 0 {
		return nil
	}
	return bigtable.TimestampRangeFilterMicros(bigtable.Timestamp(q.StartTimestampMs*1000), bigtable.Timestamp(q.EndTimestampMs*1000))
}

// QueryToBigtableColumnFilter -
func QueryToBigtableColumnFilter(q *prompb.Query) bigtable.Filter {
	if q.StartTimestampMs == 0 && q.EndTimestampMs == 0 {
		return nil
	}
	var begin, end string
	if q.StartTimestampMs != 0 {
		begin = string(Int64ToBytes(q.StartTimestampMs))
	}
	if q.EndTimestampMs != 0 {
		end = string(Int64ToBytes(q.EndTimestampMs + 1))
	}
	return bigtable.ColumnRangeFilter(metricFamily, begin, end)
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

// ParseRowKey -
func ParseRowKey(rk string) (name string, metricID MetricIdentifier, labels Labels, ok bool) {
	parts := strings.Split(rk, "#")
	if len(parts) != 4 {
		return "", "", nil, false
	}

	name = parts[1]
	labelsString := parts[3]
	metricID = MetricIdentifier(strings.Join([]string{name, labelsString}, "#"))
	lss := strings.Split(labelsString, ",")
	labels = make(Labels, len(lss))
	for i := range lss {
		lparts := strings.Split(lss[i], "=")
		if len(lparts) != 2 {
			return name, "", labels, false
		}
		labels[i] = prompb.Label{Name: lparts[0], Value: UnescapeLabelValue(lparts[1])}
	}
	sort.Sort(labels)
	return name, "", labels, true
}
