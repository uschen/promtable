package promtable

import (
	"regexp"
	"sort"

	"github.com/uschen/promtable/prompb"
)

// QueryMatchersToLabelsMatcher - qm must be sorted
func QueryMatchersToLabelsMatcher(qm []*prompb.LabelMatcher) (lsm LabelsMatcher, err error) {
	if len(qm) == 0 {
		return
	}
	for i := range qm {
		lm, err := queryMatcherToLabelMatcher(qm[i])
		if err != nil {
			return nil, err
		}

		lsm = append(lsm, lm)
	}
	sort.Sort(lsm)
	return
}

func queryMatcherToLabelMatcher(m *prompb.LabelMatcher) (*labelMatcher, error) {
	var (
		not bool
		lv  = m.Value
	)
	lm := &labelMatcher{
		Name: m.Name,
	}

	switch m.Type {
	case prompb.LabelMatcher_NEQ:
		lm.matchFunc = func(v string) bool {
			return v != lv
		}
	case prompb.LabelMatcher_EQ:
		lm.matchFunc = func(v string) bool {
			return v == lv
		}
	case prompb.LabelMatcher_NRE:
		not = true
		fallthrough
	case prompb.LabelMatcher_RE:
		regexMatch, err := regexp.Compile(lv)
		if err != nil {
			return nil, err
		}
		if !not {
			lm.matchFunc = func(v string) bool {
				return regexMatch.MatchString(v)
			}
		} else {
			lm.matchFunc = func(v string) bool {
				return !regexMatch.MatchString(v)
			}
		}
	}

	return lm, nil
}

type valueMatchFunc func(value string) bool

// labelMatcher -
type labelMatcher struct {
	Name      string
	matchFunc valueMatchFunc
}

// LabelsMatcher - labelTester is sorted by name
type LabelsMatcher []*labelMatcher

func (m LabelsMatcher) Len() int           { return len(m) }
func (m LabelsMatcher) Less(i, j int) bool { return m[i].Name < m[j].Name }
func (m LabelsMatcher) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// Match - labels must be sorted
func (m LabelsMatcher) Match(labels []prompb.Label) (ok bool) {
	if len(m) == 0 {
		// nothing to match with
		return true
	}
	if len(labels) == 0 {
		// has matcher but no labels
		return false
	}
	// every sinlge one of matchers has to be satisfied
	// matchers: e e g h
	// labels:   c d e f g h i j k
	var (
		i, k int
	)
	for i < len(m) {
		if m[i].Name < labels[k].Name {
			return false
		}
		if m[i].Name == labels[k].Name {
			if !m[i].matchFunc(labels[k].Value) {
				// not match...
				return false
			}
			// matched,
			// the next matcher might have the same name, don't move k yet
			k++
			i++
			continue
		}
		// >
		if k == len(labels)-1 { // this ends main loop
			// k is the last one already...
			return false
		}
		// m.Name > label.Name, needs to bump k
		k++
	}
	return true
}
