package promtable_test

import (
	"encoding/binary"
	"math"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/uschen/promtable"
)

func f64(val float64) [binary.MaxVarintLen64]byte {
	var typeBuf [binary.MaxVarintLen64]byte
	binary.BigEndian.PutUint64(typeBuf[:], math.Float64bits(val))
	return typeBuf
}

func TestA(t *testing.T) {
	e := []int64{0, 1, 2, 3, 4, 5, 6}
	assert.EqualValues(t, []int64{2, 3, 4, 5, 6}, e[2:])
}

func TestFloat64ToBytes(t *testing.T) {
	a := f64(math.Pi)
	b := f64(math.Phi)
	assert.EqualValues(t, promtable.Float64ToBytes(math.Pi), a[:8])
	assert.EqualValues(t, promtable.Float64ToBytes(math.Phi), b[:8])
	assert.EqualValues(t, math.Pi, promtable.Float64FromBytes(promtable.Float64ToBytes(math.Pi)))
	assert.EqualValues(t, math.Phi, promtable.Float64FromBytes(promtable.Float64ToBytes(math.Phi)))
}

func BenchmarkFloat64ToBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mut := bigtable.NewMutation()
		a := promtable.Float64ToBytes(math.Pi)
		mut.Set("a", "b", bigtable.ServerTime, a)
	}
}

func BenchmarkFloat64FromBytes(b *testing.B) {
	a := promtable.Float64ToBytes(math.Pi)
	for i := 0; i < b.N; i++ {
		b := promtable.Float64FromBytes(a)
		_ = b
	}
}

func BenchmarkFloat64ToBytesArray(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mut := bigtable.NewMutation()
		a := f64(math.Pi)
		mut.Set("a", "b", bigtable.ServerTime, a[:])
		// b := a[:]
		// _ = b
	}
}

func BenchmarkInt64ToBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = promtable.Int64ToBytes(int64(i))
	}
}
