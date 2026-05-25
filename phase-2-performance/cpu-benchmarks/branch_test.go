package cpubenchmarks

import (
	"math/rand"
	"sort"
	"testing"
)

const branchSize = 1 << 17

func BenchmarkSortedBranch(b *testing.B) {
	data := make([]int, branchSize)
	for i := range data {
		data[i] = rand.Intn(256)
	}
	sort.Ints(data)

	b.ResetTimer()
	var sum int64
	for n := 0; n < b.N; n++ {
		for _, v := range data {
			if v >= 128 {
				sum += int64(v)
			}
		}
	}
	_ = sum
}

func BenchmarkUnsortedBranch(b *testing.B) {
	data := make([]int, branchSize)
	for i := range data {
		data[i] = rand.Intn(256)
	}

	b.ResetTimer()
	var sum int64
	for n := 0; n < b.N; n++ {
		for _, v := range data {
			if v >= 128 {
				sum += int64(v)
			}
		}
	}
	_ = sum
}
