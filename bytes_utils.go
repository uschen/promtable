package promtable

import (
	"encoding/binary"
	"math"
)

// Float64ToBytes -
func Float64ToBytes(val float64) []byte {
	typeBuf := make([]byte, binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(typeBuf[:], math.Float64bits(val))
	return typeBuf
}

// Float64FromBytes -
func Float64FromBytes(val []byte) float64 {
	if len(val) == 0 {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(val))
}

// Int64ToBytes -
func Int64ToBytes(val int64) []byte {
	typeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(typeBuf, uint64(val))
	return typeBuf
}

// Int64FromBytes -
func Int64FromBytes(val []byte) int64 {
	if len(val) == 0 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val))
}
