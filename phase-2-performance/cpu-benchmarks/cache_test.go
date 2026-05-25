package cpubenchmarks

import (
	"math/rand"
	"testing"
)

const arraySize = 64 * 1024 * 1024

func BenchmarkSequentialAccess(b *testing.B) {
	data := make([]int64, arraySize/8)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	var sum int64
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(data); i++ {
			sum += data[i]
		}
	}
	_ = sum
}

func BenchmarkRandomAccess(b *testing.B) {
	data := make([]int64, arraySize/8)
	indices := make([]int, len(data))
	for i := range indices {
		indices[i] = rand.Intn(len(data))
	}

	b.ResetTimer()
	var sum int64
	for n := 0; n < b.N; n++ {
		for _, idx := range indices {
			sum += data[idx]
		}
	}
	_ = sum
}
