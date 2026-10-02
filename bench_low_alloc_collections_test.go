package aerospike

import (
	"fmt"
	"testing"
)

type bigListEncoder struct{ list ListIter }

func (e *bigListEncoder) WriteBins(w BinWriter) Error { return w.WriteList("values", e.list) }

type bigListSizedEncoder struct {
	bigListEncoder
	size int
}

func (e *bigListSizedEncoder) EncodedBinsSizeHint() int { return e.size }

// Large list: cost of the size pass in WriteList.
func BenchmarkLowAllocWriteBigList(b *testing.B) {
	policy := NewWritePolicy(0, 0)
	key, _ := NewKey("test", "bench", "k")
	for _, n := range []int{16, 1024, 16384} {
		list := make(mockIntList, n)
		for i := range list {
			list[i] = i * 31
		}
		size, _ := PackList(nil, list)
		buf := make([]byte, 2*size+256)

		b.Run(fmt.Sprintf("bins_listervalue/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			bins := []*Bin{{Name: "values", Value: NewListerValue(list)}}
			for i := 0; i < b.N; i++ {
				cmd, _ := newWriteCommand(nil, policy, key, bins, nil, nil, _WRITE)
				cmd.baseCommand.dataBuffer = buf
				if err := cmd.writeBuffer(&cmd); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("encoded_writelist/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			enc := &bigListEncoder{list}
			for i := 0; i < b.N; i++ {
				cmd, _ := newWriteCommand(nil, policy, key, nil, nil, enc, _WRITE)
				cmd.baseCommand.dataBuffer = buf
				if err := cmd.writeBuffer(&cmd); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Buffer growth: no usable size hint and an empty starting buffer.
func BenchmarkLowAllocWriteGrowth(b *testing.B) {
	policy := NewWritePolicy(0, 0)
	key, _ := NewKey("test", "bench", "k")
	list := make(mockIntList, 1024)
	size, _ := PackList(nil, list)

	b.Run("no_hint", func(b *testing.B) {
		b.ReportAllocs()
		enc := &bigListEncoder{list}
		for i := 0; i < b.N; i++ {
			cmd, _ := newWriteCommand(nil, policy, key, nil, nil, enc, _WRITE)
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
			buffPool.Put(cmd.baseCommand.dataBuffer)
		}
	})
	b.Run("exact_hint", func(b *testing.B) {
		b.ReportAllocs()
		enc := &bigListSizedEncoder{bigListEncoder{list}, size + 64}
		for i := 0; i < b.N; i++ {
			cmd, _ := newWriteCommand(nil, policy, key, nil, nil, enc, _WRITE)
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
			buffPool.Put(cmd.baseCommand.dataBuffer)
		}
	})
}
