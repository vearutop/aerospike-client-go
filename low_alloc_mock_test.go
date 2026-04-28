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
	"reflect"
	"testing"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type mockPayloadBin struct {
	name  string
	value Value
}

func buildMockRecordPayload(tb testing.TB, bins []mockPayloadBin) []byte {
	tb.Helper()

	totalSize := 0
	for i := range bins {
		sz, err := bins[i].value.EstimateSize()
		if err != nil {
			tb.Fatalf("estimate size for %s: %v", bins[i].name, err)
		}
		totalSize += 4 + 4 + len(bins[i].name) + sz
	}

	buf := newBuffer(totalSize)
	for i := range bins {
		sz, err := bins[i].value.EstimateSize()
		if err != nil {
			tb.Fatalf("estimate size for %s: %v", bins[i].name, err)
		}

		buf.WriteInt32(int32(4 + len(bins[i].name) + sz))
		buf.WriteByte(_READ.op)
		buf.WriteByte(byte(bins[i].value.GetType()))
		buf.WriteByte(0)
		buf.WriteByte(byte(len(bins[i].name)))
		if _, err := buf.WriteString(bins[i].name); err != nil {
			tb.Fatalf("write name for %s: %v", bins[i].name, err)
		}
		if _, err := bins[i].value.write(buf); err != nil {
			tb.Fatalf("write value for %s: %v", bins[i].name, err)
		}
	}

	return append([]byte(nil), buf.Bytes()...)
}

type mockCallbackReceiver struct {
	generation uint32
	expiration uint32
	id         int64
	name       string
	score      int64
}

func (m *mockCallbackReceiver) SetHeader(generation uint32, expiration uint32) {
	m.generation = generation
	m.expiration = expiration
}

func (m *mockCallbackReceiver) SetBin(name []byte, value RawBinValue) Error {
	switch string(name) {
	case "id":
		v, ok := value.Int64()
		if !ok {
			return ErrInvalidObjectType
		}
		m.id = v
	case "name":
		v, ok := value.String()
		if !ok {
			return ErrInvalidObjectType
		}
		m.name = v
	case "score":
		v, ok := value.Int64()
		if !ok {
			return ErrInvalidObjectType
		}
		m.score = v
	}

	return nil
}

type mockObject struct {
	TTL   uint32 `asm:"ttl"`
	Gen   uint32 `asm:"gen"`
	ID    int    `as:"id"`
	Name  string `as:"name"`
	Score int64  `as:"score"`
}

type mockWriteIter struct {
	id    int
	name  string
	score int64
}

func (m *mockWriteIter) Len() int {
	return 3
}

func (m *mockWriteIter) EstimateBin(i int) (string, int, int, Error) {
	switch i {
	case 0:
		return "id", ParticleType.INTEGER, 8, nil
	case 1:
		return "name", ParticleType.STRING, len(m.name), nil
	case 2:
		return "score", ParticleType.INTEGER, 8, nil
	default:
		return "", 0, 0, ErrInvalidObjectType
	}
}

func (m *mockWriteIter) WriteBin(i int, cmd BufferEx) (int, Error) {
	switch i {
	case 0:
		return cmd.WriteInt64(int64(m.id)), nil
	case 1:
		return cmd.WriteString(m.name)
	case 2:
		return cmd.WriteInt64(m.score), nil
	default:
		return 0, ErrInvalidObjectType
	}
}

func TestMockReadPathsMatch(t *testing.T) {
	payload := buildMockRecordPayload(t, []mockPayloadBin{
		{name: "id", value: IntegerValue(7)},
		{name: "name", value: StringValue("alpha")},
		{name: "score", value: LongValue(99)},
	})

	key, err := NewKey("test", "set", "user-1")
	if err != nil {
		t.Fatalf("new key: %v", err)
	}

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
	rec, err := rp.parseRecord(key, false)
	if err != nil {
		t.Fatalf("parse record: %v", err)
	}

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
	if err := parseObject(brc, 3, 0, 11, 22); err != nil {
		t.Fatalf("parse object: %v", err)
	}

	var cb mockCallbackReceiver
	rp2 := &recordParser{
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
	if err := rp2.parseRecordInto(&cb); err != nil {
		t.Fatalf("parse callback: %v", err)
	}

	if got := rec.Bins["id"]; got != 7 {
		t.Fatalf("record id = %v", got)
	}
	if got := rec.Bins["name"]; got != "alpha" {
		t.Fatalf("record name = %v", got)
	}
	if got := rec.Bins["score"]; got != 99 {
		t.Fatalf("record score = %v", got)
	}

	if obj.ID != 7 || obj.Name != "alpha" || obj.Score != 99 {
		t.Fatalf("object = %+v", obj)
	}
	if obj.Gen != 11 || obj.TTL != 22 {
		t.Fatalf("object header = gen:%d ttl:%d", obj.Gen, obj.TTL)
	}

	if cb.id != 7 || cb.name != "alpha" || cb.score != 99 {
		t.Fatalf("callback = %+v", cb)
	}
	if cb.generation != 11 || cb.expiration != 22 {
		t.Fatalf("callback header = gen:%d ttl:%d", cb.generation, cb.expiration)
	}
}

func TestMockWriteIterMatchesBins(t *testing.T) {
	policy := NewWritePolicy(0, 0)
	key, err := NewKey("test", "set", "user-1")
	if err != nil {
		t.Fatalf("new key: %v", err)
	}

	bins := []*Bin{
		{Name: "id", Value: IntegerValue(7)},
		{Name: "name", Value: StringValue("alpha")},
		{Name: "score", Value: LongValue(99)},
	}

	iter := mockWriteIter{id: 7, name: "alpha", score: 99}

	cmdBins, err := newWriteCommand(nil, policy, key, bins, nil, nil, _WRITE)
	if err != nil {
		t.Fatalf("new bins command: %v", err)
	}
	cmdBins.baseCommand.dataBuffer = make([]byte, 1024)
	if err := cmdBins.writeBuffer(&cmdBins); err != nil {
		t.Fatalf("write bins buffer: %v", err)
	}

	cmdIter, err := newWriteCommand(nil, policy, key, nil, nil, &iter, _WRITE)
	if err != nil {
		t.Fatalf("new iter command: %v", err)
	}
	cmdIter.baseCommand.dataBuffer = make([]byte, 1024)
	if err := cmdIter.writeBuffer(&cmdIter); err != nil {
		t.Fatalf("write iter buffer: %v", err)
	}

	if !reflect.DeepEqual(cmdBins.dataBuffer[:cmdBins.dataOffset], cmdIter.dataBuffer[:cmdIter.dataOffset]) {
		t.Fatalf("iter payload does not match bins payload")
	}
}
