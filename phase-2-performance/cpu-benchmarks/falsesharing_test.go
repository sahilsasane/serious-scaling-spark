package cpubenchmarks

import (
	"sync"
	"testing"
)

type SharedCounters struct {
	a int64
	b int64
}

type PaddedCounters struct {
	a int64
	_ [56]byte // pad to 64 bytes
	b int64
	_ [56]byte
}

func BenchmarkFalseSharing(b *testing.B) {
	c := &SharedCounters{}
	var wg sync.WaitGroup

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c.a, c.b = 0, 0
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				c.a++
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				c.b++
			}
		}()
		wg.Wait()
	}
}

func BenchmarkNoPadding(b *testing.B) {
	c := &PaddedCounters{}
	var wg sync.WaitGroup

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c.a, c.b = 0, 0
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				c.a++
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				c.b++
			}
		}()
		wg.Wait()
	}
}
