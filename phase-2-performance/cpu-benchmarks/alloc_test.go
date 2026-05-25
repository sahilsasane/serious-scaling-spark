package cpubenchmarks

import (
	"sync"
	"testing"
)

const bufSize = 4096

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufSize)
		return &b
	},
}

func BenchmarkAllocEveryTime(b *testing.B) {
	b.ReportAllocs()
	var sink []byte
	for n := 0; n < b.N; n++ {
		buf := make([]byte, bufSize)
		buf[0] = 1
		sink = buf
	}
	_ = sink
}

func BenchmarkPooledAlloc(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		bufPtr := bufPool.Get().(*[]byte)
		buf := *bufPtr
		buf[0] = 1
		bufPool.Put(bufPtr)
	}
}
