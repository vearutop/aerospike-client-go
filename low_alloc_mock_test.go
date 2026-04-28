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

func (m *mockCallbackReceiver) SetBin(name []byte, value RawValue) Error {
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

type mockCollectionCallbackReceiver struct {
	ratio32  float32
	unsigned uint64
	nullSet  bool
	geo      string
	hll      []byte
	tags     map[string]string
	values   []int64
}

func (m *mockCollectionCallbackReceiver) SetBin(name []byte, value RawValue) Error {
	switch string(name) {
	case "ratio32":
		v, ok := value.Float32()
		if !ok {
			return ErrInvalidObjectType
		}
		m.ratio32 = v
	case "unsigned":
		v, ok := value.Uint64()
		if !ok {
			return ErrInvalidObjectType
		}
		m.unsigned = v
	case "none":
		m.nullSet = value.IsNull()
	case "geo":
		v, ok := value.GeoJSON()
		if !ok {
			return ErrInvalidObjectType
		}
		m.geo = v
	case "hll":
		v, ok := value.HLL()
		if !ok {
			return ErrInvalidObjectType
		}
		m.hll = append(m.hll[:0], v...)
	case "tags":
		m.tags = make(map[string]string)
		if err := value.ForEachMap(func(key RawValue, val RawValue) Error {
			k, ok := key.String()
			if !ok {
				return ErrInvalidObjectType
			}
			v, ok := val.String()
			if !ok {
				return ErrInvalidObjectType
			}
			m.tags[k] = v
			return nil
		}); err != nil {
			return err
		}
	case "values":
		m.values = m.values[:0]
		if err := value.ForEachList(func(elem RawValue) Error {
			v, ok := elem.Int64()
			if !ok {
				return ErrInvalidObjectType
			}
			m.values = append(m.values, v)
			return nil
		}); err != nil {
			return err
		}
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
	id       int
	name     string
	score    int64
	ratio32  float32
	unsigned uint64
	nullSet  bool
	geo      string
	hll      []byte
	tags     mockStringMap
	values   mockIntList
}

func (m *mockWriteIter) EncodedBinsSizeHint() int {
	tagsSize, _ := PackMap(nil, m.tags)
	valuesSize, _ := PackList(nil, m.values)
	return 10*int(_OPERATION_HEADER_SIZE) +
		len("id") + len("name") + len("score") + len("ratio32") + len("unsigned") + len("none") + len("geo") + len("hll") + len("tags") + len("values") +
		8 + len(m.name) + 8 + 4 + 8 + 0 + 3 + len(m.geo) + len(m.hll) + tagsSize + valuesSize
}

func (m *mockWriteIter) WriteBins(w BinWriter) Error {
	if err := w.WriteInt("id", m.id); err != nil {
		return err
	}
	if err := w.WriteString("name", m.name); err != nil {
		return err
	}
	if err := w.WriteInt64("score", m.score); err != nil {
		return err
	}
	if err := w.WriteFloat32("ratio32", m.ratio32); err != nil {
		return err
	}
	if err := w.WriteUint64("unsigned", m.unsigned); err != nil {
		return err
	}
	if m.nullSet {
		if err := w.WriteNull("none"); err != nil {
			return err
		}
	}
	if err := w.WriteGeoJSON("geo", m.geo); err != nil {
		return err
	}
	if err := w.WriteHLL("hll", m.hll); err != nil {
		return err
	}
	if err := w.WriteMap("tags", m.tags); err != nil {
		return err
	}
	return w.WriteList("values", m.values)
}

type mockStringMap map[string]string

func (m mockStringMap) PackMap(buf BufferEx) (int, error) {
	size := 0
	for key, value := range m {
		n, err := PackString(buf, key)
		size += n
		if err != nil {
			return size, err
		}

		n, err = PackString(buf, value)
		size += n
		if err != nil {
			return size, err
		}
	}

	return size, nil
}

func (m mockStringMap) Len() int {
	return len(m)
}

type mockIntList []int

func (l mockIntList) PackList(buf BufferEx) (int, error) {
	size := 0
	for _, value := range l {
		n, err := PackInt64(buf, int64(value))
		size += n
		if err != nil {
			return size, err
		}
	}

	return size, nil
}

func (l mockIntList) Len() int {
	return len(l)
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
		{Name: "ratio32", Value: NewRawBlobValue(ParticleType.FLOAT, []byte{0x40, 0x60, 0x00, 0x00})},
		{Name: "unsigned", Value: NewRawBlobValue(ParticleType.INTEGER, []byte{0, 0, 0, 0, 0, 0, 0, 42})},
		{Name: "none", Value: NewNullValue()},
		{Name: "geo", Value: NewGeoJSONValue(`{"type":"Point","coordinates":[1.0,2.0]}`)},
		{Name: "hll", Value: NewHLLValue([]byte{1, 2, 3, 4})},
		{Name: "tags", Value: NewMapperValue(mockStringMap{"env": "prod", "region": "eu"})},
		{Name: "values", Value: NewListerValue(mockIntList{1, 3, 5})},
	}

	iter := mockWriteIter{
		id:       7,
		name:     "alpha",
		score:    99,
		ratio32:  3.5,
		unsigned: 42,
		nullSet:  true,
		geo:      `{"type":"Point","coordinates":[1.0,2.0]}`,
		hll:      []byte{1, 2, 3, 4},
		tags:     mockStringMap{"env": "prod", "region": "eu"},
		values:   mockIntList{1, 3, 5},
	}

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

func TestMockReadTypedCollections(t *testing.T) {
	payload := buildMockRecordPayload(t, []mockPayloadBin{
		{name: "ratio32", value: NewRawBlobValue(ParticleType.FLOAT, []byte{0x40, 0x60, 0x00, 0x00})},
		{name: "unsigned", value: NewRawBlobValue(ParticleType.INTEGER, []byte{0, 0, 0, 0, 0, 0, 0, 42})},
		{name: "none", value: NewNullValue()},
		{name: "geo", value: NewGeoJSONValue(`{"type":"Point","coordinates":[1.0,2.0]}`)},
		{name: "hll", value: NewHLLValue([]byte{1, 2, 3, 4})},
		{name: "tags", value: NewMapperValue(mockStringMap{"env": "prod", "region": "eu"})},
		{name: "values", value: NewListerValue(mockIntList{1, 3, 5})},
	})

	var cb mockCollectionCallbackReceiver
	rp := &recordParser{
		generation: 11,
		expiration: 22,
		opCount:    7,
		cmd: &baseCommand{
			bufferEx: bufferEx{
				dataBuffer: payload,
				dataOffset: 0,
			},
		},
	}

	if err := rp.parseRecordInto(&cb); err != nil {
		t.Fatalf("parse callback: %v", err)
	}

	if cb.ratio32 != 3.5 {
		t.Fatalf("ratio32 = %v", cb.ratio32)
	}
	if cb.unsigned != 42 {
		t.Fatalf("unsigned = %d", cb.unsigned)
	}
	if !cb.nullSet {
		t.Fatalf("null was not detected")
	}
	if cb.geo != `{"type":"Point","coordinates":[1.0,2.0]}` {
		t.Fatalf("geo = %q", cb.geo)
	}
	if !reflect.DeepEqual(cb.hll, []byte{1, 2, 3, 4}) {
		t.Fatalf("hll = %v", cb.hll)
	}
	if !reflect.DeepEqual(cb.tags, map[string]string{"env": "prod", "region": "eu"}) {
		t.Fatalf("tags = %#v", cb.tags)
	}
	if !reflect.DeepEqual(cb.values, []int64{1, 3, 5}) {
		t.Fatalf("values = %v", cb.values)
	}
}
