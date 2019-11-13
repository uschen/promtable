package promtable

import (
	"encoding/binary"
	"math"
)

// Float64ToBytes -
func Float64ToBytes(val float64) []byte {
	typeBuf := make([]byte, 8)
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

// Int32ToBytes -
func Int32ToBytes(val int32) []byte {
	typeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBuf, uint32(val))
	return typeBuf
}

// Int32FromBytes converts BigEdian bytes encoding
// to int32. bytes can be either 8 bytes or 10 bytes.
func Int32FromBytes(val []byte) int32 {
	if len(val) == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(val))
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
