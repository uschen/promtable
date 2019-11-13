package btrowset

import (
	"bytes"

	"cloud.google.com/go/bigtable"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// RowSet implements bigtable.RowSet interface.
type RowSet btpb.RowSet

// Proto -
func (r RowSet) Proto() *btpb.RowSet {
	rs := btpb.RowSet(r)
	return &rs
}

// RetainRowsAfter -
func (r RowSet) RetainRowsAfter(lastRowKey string) bigtable.RowSet {
	res := RowSet{}
	for i := range r.RowKeys {
		if string(r.RowKeys[i]) > lastRowKey {
			res.RowKeys = append(res.RowKeys, r.RowKeys[i])
		}
	}
	for i := range r.RowRanges {
		var (
			newRange = &btpb.RowRange{
				StartKey: r.RowRanges[i].StartKey,
				EndKey:   r.RowRanges[i].EndKey,
			}
		)

		switch rr := r.RowRanges[i].EndKey.(type) {
		case *btpb.RowRange_EndKeyOpen:
			if string(rr.EndKeyOpen) <= lastRowKey {
				continue
			}
		case *btpb.RowRange_EndKeyClosed:
			if string(rr.EndKeyClosed) <= lastRowKey {
				continue
			}
		}

		switch rr := r.RowRanges[i].StartKey.(type) {
		case *btpb.RowRange_StartKeyOpen:
			if string(rr.StartKeyOpen) <= lastRowKey {
				newRange.StartKey = &btpb.RowRange_StartKeyOpen{
					StartKeyOpen: []byte(lastRowKey),
				}
			}
		case *btpb.RowRange_StartKeyClosed:
			if string(rr.StartKeyClosed) <= lastRowKey {
				newRange.StartKey = &btpb.RowRange_StartKeyOpen{
					StartKeyOpen: []byte(lastRowKey),
				}
			}
		}

		res.RowRanges = append(res.RowRanges, newRange)

	}
	return res
}

// Valid -
func (r RowSet) Valid() bool {
	if len(r.RowKeys) > 0 {
		return true
	}
	if len(r.RowKeys) == 0 && len(r.RowRanges) == 0 {
		return false
	}

	for i := range r.RowRanges {
		if r.RowRanges[i].StartKey == nil || r.RowRanges[i].EndKey == nil {
			return true
		}
		if btRowRangeValid(r.RowRanges[i]) {
			return true
		}
	}

	return false
}

func btRowRangeValid(rr *btpb.RowRange) bool {
	if rr.StartKey == nil || rr.EndKey == nil {
		return true
	}
	switch rs := rr.StartKey.(type) {
	case *btpb.RowRange_StartKeyOpen:
		switch re := rr.EndKey.(type) {
		case *btpb.RowRange_EndKeyOpen:
			return bytes.Compare(rs.StartKeyOpen, re.EndKeyOpen) < 0
		case *btpb.RowRange_EndKeyClosed:
			return bytes.Compare(rs.StartKeyOpen, re.EndKeyClosed) < 0
		}
	case *btpb.RowRange_StartKeyClosed:
		switch re := rr.EndKey.(type) {
		case *btpb.RowRange_EndKeyOpen:
			return bytes.Compare(rs.StartKeyClosed, re.EndKeyOpen) < 0
		case *btpb.RowRange_EndKeyClosed:
			return bytes.Compare(rs.StartKeyClosed, re.EndKeyClosed) <= 0
		}
	}
	return false
}
