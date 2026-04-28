//go:build !as_performance

// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	benchmarkLowAllocRecord   *Record
	benchmarkLowAllocObject   mockObject
	benchmarkLowAllocCallback mockCallbackReceiver
	benchmarkLowAllocErr      Error
	benchmarkLowAllocPayload  []byte
)

func benchmarkMockObjects(size int) []mockObject {
	objects := make([]mockObject, size)
	for i := range objects {
		objects[i] = mockObject{
			ID:    i + 1,
			Name:  fmt.Sprintf("user-%04d", i),
			Score: int64(i*17 + 99),
		}
	}

	return objects
}

func BenchmarkLowAllocWritePaths(b *testing.B) {
	policy := NewWritePolicy(0, 0)
	key, _ := NewKey("test", "bench", "user-1")
	bufferSize := 1024
	objects := benchmarkMockObjects(1024)

	b.Run("bins_reused", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		bins := []*Bin{
			{Name: "id"},
			{Name: "name"},
			{Name: "score"},
		}
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			bins[0].Value = IntegerValue(obj.ID)
			bins[1].Value = StringValue(obj.Name)
			bins[2].Value = LongValue(obj.Score)

			cmd, err := newWriteCommand(nil, policy, key, bins, nil, nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("bins_fresh", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			bins := []*Bin{
				{Name: "id", Value: IntegerValue(obj.ID)},
				{Name: "name", Value: StringValue(obj.Name)},
				{Name: "score", Value: LongValue(obj.Score)},
			}

			cmd, err := newWriteCommand(nil, policy, key, bins, nil, nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	// BenchmarkLowAllocWritePaths/iter_fast-14     	 8057196	       150.6 ns/op	     208 B/op	       1 allocs/op
	// BenchmarkLowAllocWritePaths/iter_fast-14     	 7180738	       171.8 ns/op	     240 B/op	       2 allocs/op
	b.Run("iter_fast", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		iter := mockWriteIter{}
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			iter.id = obj.ID
			iter.name = obj.Name
			iter.score = obj.Score

			cmd, err := newWriteCommand(nil, policy, key, nil, nil, &iter, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("iter_boxed", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		iter := mockBoxedWriteIter{}
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			iter.id = obj.ID
			iter.name = obj.Name
			iter.score = obj.Score

			cmd, err := newWriteCommand(nil, policy, key, nil, nil, iter, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("binmap_fresh", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			binMap := BinMap{
				"id":    obj.ID,
				"name":  obj.Name,
				"score": obj.Score,
			}

			cmd, err := newWriteCommand(nil, policy, key, nil, binMap, nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("reflect_fresh", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			obj := objects[i%len(objects)]
			cmd, err := newWriteCommand(nil, policy, key, nil, marshal(obj), nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkLowAllocWritePathsSteadyState(b *testing.B) {
	policy := NewWritePolicy(0, 0)
	key, _ := NewKey("test", "bench", "user-1")
	bufferSize := 1024

	obj := mockObject{
		ID:    7,
		Name:  "alpha",
		Score: 99,
	}
	binMap := BinMap{
		"id":    7,
		"name":  "alpha",
		"score": int64(99),
	}
	bins := []*Bin{
		{Name: "id", Value: IntegerValue(7)},
		{Name: "name", Value: StringValue("alpha")},
		{Name: "score", Value: LongValue(99)},
	}
	iter := mockWriteIter{id: 7, name: "alpha", score: 99}
	boxedIter := mockBoxedWriteIter{id: 7, name: "alpha", score: 99}
	preboxedIter := mockPreboxedWriteIter{
		names: [3]string{"id", "name", "score"},
		values: [3]Value{
			IntegerValue(7),
			StringValue("alpha"),
			LongValue(99),
		},
	}

	b.Run("bins", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, bins, nil, nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("binmap", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, nil, binMap, nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("iter_fast", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, nil, nil, &iter, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("iter_boxed", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, nil, nil, boxedIter, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("iter_preboxed", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, nil, nil, preboxedIter, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("reflect", func(b *testing.B) {
		b.ReportAllocs()
		dataBuffer := make([]byte, bufferSize)
		for i := 0; i < b.N; i++ {
			cmd, err := newWriteCommand(nil, policy, key, nil, marshal(obj), nil, _WRITE)
			if err != nil {
				b.Fatal(err)
			}
			cmd.baseCommand.dataBuffer = dataBuffer
			if err := cmd.writeBuffer(&cmd); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkLowAllocReadPaths(b *testing.B) {
	payload := buildMockRecordPayload(b, []mockPayloadBin{
		{name: "id", value: IntegerValue(7)},
		{name: "name", value: StringValue("alpha")},
		{name: "score", value: LongValue(99)},
	})
	key, _ := NewKey("test", "bench", "user-1")

	b.Run("record_bins", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rp := &recordParser{
				generation: 11,
				expiration: 22,
				opCount:    3,
				cmd: &baseCommand{
					bufferEx: bufferEx{
						dataBuffer: payload,
						dataOffset: 0,
					},
				},
			}
			benchmarkLowAllocRecord, benchmarkLowAllocErr = rp.parseRecord(key, false)
			if benchmarkLowAllocErr != nil {
				b.Fatal(benchmarkLowAllocErr)
			}
		}
	})

	b.Run("reflection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var obj mockObject
			rv := reflect.ValueOf(&obj)
			brc := &baseReadCommand{
				singleCommand: singleCommand{
					baseCommand: baseCommand{
						bufferEx: bufferEx{
							dataBuffer: payload,
							dataOffset: 0,
						},
					},
					key: key,
				},
				object: &rv,
			}
			benchmarkLowAllocErr = parseObject(brc, 3, 0, 11, 22)
			if benchmarkLowAllocErr != nil {
				b.Fatal(benchmarkLowAllocErr)
			}
			benchmarkLowAllocObject = obj
		}
	})

	b.Run("callback", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var cb mockCallbackReceiver
			rp := &recordParser{
				generation: 11,
				expiration: 22,
				opCount:    3,
				cmd: &baseCommand{
					bufferEx: bufferEx{
						dataBuffer: payload,
						dataOffset: 0,
					},
				},
			}
			benchmarkLowAllocErr = rp.parseRecordInto(&cb)
			if benchmarkLowAllocErr != nil {
				b.Fatal(benchmarkLowAllocErr)
			}
			benchmarkLowAllocCallback = cb
		}
	})

	benchmarkLowAllocPayload = payload
}
