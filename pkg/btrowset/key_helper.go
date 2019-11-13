package btrowset

import (
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// RowKey -
type RowKey []byte

// NewRowKey -
func NewRowKey(val []byte) RowKey {
	var k = make([]byte, len(val))
	copy(k, val)
	return k
}

// StringRowKey -
func StringRowKey(rk string) RowKey {
	return RowKey(rk)
}

// KeyRangeStartOpen -
func (r RowKey) KeyRangeStartOpen() *btpb.RowRange_StartKeyOpen {
	return &btpb.RowRange_StartKeyOpen{
		StartKeyOpen: r,
	}
}

// KeyRangeStartClosed -
func (r RowKey) KeyRangeStartClosed() *btpb.RowRange_StartKeyClosed {
	return &btpb.RowRange_StartKeyClosed{
		StartKeyClosed: r,
	}
}

// KeyRangeEndOpen -
func (r RowKey) KeyRangeEndOpen() *btpb.RowRange_EndKeyOpen {
	return &btpb.RowRange_EndKeyOpen{
		EndKeyOpen: r,
	}
}

// KeyRangeEndClosed -
func (r RowKey) KeyRangeEndClosed() *btpb.RowRange_EndKeyClosed {
	return &btpb.RowRange_EndKeyClosed{
		EndKeyClosed: r,
	}
}

// PrefixSuccessor -
func PrefixSuccessor(prefix RowKey) RowKey {
	res := make([]byte, len(prefix))
	copy(res, prefix)
	if len(res) == 0 {
		return res // infinite range
	}
	n := len(res)
	for n--; n >= 0 && res[n] == '\xff'; n-- {
	}
	if n == -1 {
		return nil
	}
	ans := res[:n]
	ans = append(ans, res[n]+1)
	return ans
}
