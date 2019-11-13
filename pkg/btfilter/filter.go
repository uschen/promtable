package btfilter

import (
	"time"

	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

func timeMicro(t time.Time) int64 {
	ts := t.UnixNano() / 1e3
	return ts - ts%1000
}

// RowFilter -
type RowFilter btpb.RowFilter

// String -
func (b *RowFilter) String() string {
	if b == nil {
		return new(btpb.RowFilter).String()
	}
	a := btpb.RowFilter(*b)
	return (&a).String()
}

// Proto -
func (b *RowFilter) Proto() *btpb.RowFilter {
	if b == nil {
		return nil
	}
	a := btpb.RowFilter(*b)
	return &a
}
