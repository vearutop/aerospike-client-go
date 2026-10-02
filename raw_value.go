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

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

func (rbv RawValue) packedBytesPayload() ([]byte, bool) {
	switch rbv.particleType {
	case ParticleType.BLOB, ParticleType.HLL:
		return rbv.packedBytesPayloadForType(rbv.particleType)
	}
	return nil, false
}

func (rbv RawValue) packedBytesPayloadForType(expected int) ([]byte, bool) {
	if !rbv.packed {
		return nil, false
	}
	_, payload, ok := packedValuePayload(rbv.buf, expected)
	return payload, ok
}

func (rbv RawValue) packedStringPayload(expected int) ([]byte, bool) {
	if !rbv.packed {
		return nil, false
	}
	_, payload, ok := packedValuePayload(rbv.buf, expected)
	return payload, ok
}

func (rbv RawValue) packedInt64() (int64, bool) {
	if !rbv.packed || len(rbv.buf) == 0 {
		return 0, false
	}

	switch rbv.buf[0] {
	case 0xcc:
		return int64(rbv.buf[1]), true
	case 0xcd:
		return int64(Buffer.BytesToUint16(rbv.buf, 1)), true
	case 0xce:
		return int64(Buffer.BytesToUint32(rbv.buf, 1)), true
	case 0xcf:
		return int64(uint64(Buffer.BytesToInt64(rbv.buf, 1))), true
	case 0xd0:
		return int64(int8(rbv.buf[1])), true
	case 0xd1:
		return int64(Buffer.BytesToInt16(rbv.buf, 1)), true
	case 0xd2:
		return int64(Buffer.BytesToInt32(rbv.buf, 1)), true
	case 0xd3:
		return Buffer.BytesToInt64(rbv.buf, 1), true
	default:
		if rbv.buf[0] < 0x80 {
			return int64(rbv.buf[0]), true
		}
		if rbv.buf[0] >= 0xe0 {
			return int64(int8(rbv.buf[0])), true
		}
	}

	return 0, false
}

func (rbv RawValue) packedUint64() (uint64, bool) {
	if !rbv.packed || len(rbv.buf) == 0 {
		return 0, false
	}

	switch rbv.buf[0] {
	case 0xcc:
		return uint64(rbv.buf[1]), true
	case 0xcd:
		return uint64(Buffer.BytesToUint16(rbv.buf, 1)), true
	case 0xce:
		return uint64(Buffer.BytesToUint32(rbv.buf, 1)), true
	case 0xcf:
		return uint64(Buffer.BytesToInt64(rbv.buf, 1)), true
	case 0xd0, 0xd1, 0xd2, 0xd3:
		v, ok := rbv.packedInt64()
		if !ok || v < 0 {
			return 0, false
		}
		return uint64(v), true
	default:
		if rbv.buf[0] < 0x80 {
			return uint64(rbv.buf[0]), true
		}
	}

	return 0, false
}

func (rbv RawValue) packedFloat32() (float32, bool) {
	if !rbv.packed || len(rbv.buf) == 0 {
		return 0, false
	}

	switch rbv.buf[0] {
	case 0xca:
		return Buffer.BytesToFloat32(rbv.buf, 1), true
	case 0xcb:
		return float32(Buffer.BytesToFloat64(rbv.buf, 1)), true
	}

	return 0, false
}

func (rbv RawValue) packedFloat64() (float64, bool) {
	if !rbv.packed || len(rbv.buf) == 0 {
		return 0, false
	}

	switch rbv.buf[0] {
	case 0xca:
		return float64(Buffer.BytesToFloat32(rbv.buf, 1)), true
	case 0xcb:
		return Buffer.BytesToFloat64(rbv.buf, 1), true
	}

	return 0, false
}

func (rbv RawValue) packedBool() (bool, bool) {
	if !rbv.packed || len(rbv.buf) != 1 {
		return false, false
	}
	switch rbv.buf[0] {
	case 0xc2:
		return false, true
	case 0xc3:
		return true, true
	}
	return false, false
}

func packedValuePayload(buf []byte, expected int) (int, []byte, bool) {
	if len(buf) == 0 {
		return 0, nil, false
	}

	theType := buf[0]
	var count int
	var headerSize int

	switch {
	case (theType & 0xe0) == 0xa0:
		count = int(theType & 0x1f)
		headerSize = 1
	case theType == 0xd9 || theType == 0xc4:
		if len(buf) < 2 {
			return 0, nil, false
		}
		count = int(buf[1])
		headerSize = 2
	case theType == 0xda || theType == 0xc5:
		if len(buf) < 3 {
			return 0, nil, false
		}
		count = int(Buffer.BytesToUint16(buf, 1))
		headerSize = 3
	case theType == 0xdb || theType == 0xc6:
		if len(buf) < 5 {
			return 0, nil, false
		}
		count = int(Buffer.BytesToUint32(buf, 1))
		headerSize = 5
	default:
		return 0, nil, false
	}

	if count < 1 || len(buf) < headerSize+count {
		return 0, nil, false
	}

	particleType := int(buf[headerSize])
	if particleType != expected {
		return 0, nil, false
	}

	return particleType, buf[headerSize+1 : headerSize+count], true
}

func forEachRawList(buf []byte, fn func(RawValue) Error) Error {
	count, offset, err := packedListHeader(buf)
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		value, next, skip, err := scanRawPackedValue(buf, offset)
		if err != nil {
			return err
		}
		offset = next
		if skip {
			continue
		}
		if err := fn(value); err != nil {
			return err
		}
	}

	return nil
}

func forEachRawMap(buf []byte, fn func(RawValue, RawValue) Error) Error {
	count, offset, err := packedMapHeader(buf)
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		key, next, skipKey, err := scanRawPackedValue(buf, offset)
		if err != nil {
			return err
		}
		offset = next

		value, next, skipValue, err := scanRawPackedValue(buf, offset)
		if err != nil {
			return err
		}
		offset = next

		if skipKey || skipValue {
			continue
		}

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

func packedListHeader(buf []byte) (count int, offset int, err Error) {
	if len(buf) == 0 {
		return 0, 0, nil
	}

	theType := buf[0]
	switch {
	case (theType & 0xf0) == 0x90:
		return int(theType & 0x0f), 1, nil
	case theType == 0xdc:
		if len(buf) < 3 {
			return 0, 0, newError(types.PARSE_ERROR)
		}
		return int(Buffer.BytesToUint16(buf, 1)), 3, nil
	case theType == 0xdd:
		if len(buf) < 5 {
			return 0, 0, newError(types.PARSE_ERROR)
		}
		return int(Buffer.BytesToUint32(buf, 1)), 5, nil
	default:
		return 0, 0, ErrInvalidObjectType
	}
}

func packedMapHeader(buf []byte) (count int, offset int, err Error) {
	if len(buf) == 0 {
		return 0, 0, nil
	}

	theType := buf[0]
	switch {
	case (theType & 0xf0) == 0x80:
		return int(theType & 0x0f), 1, nil
	case theType == 0xde:
		if len(buf) < 3 {
			return 0, 0, newError(types.PARSE_ERROR)
		}
		return int(Buffer.BytesToUint16(buf, 1)), 3, nil
	case theType == 0xdf:
		if len(buf) < 5 {
			return 0, 0, newError(types.PARSE_ERROR)
		}
		return int(Buffer.BytesToUint32(buf, 1)), 5, nil
	default:
		return 0, 0, ErrInvalidObjectType
	}
}

// scanRawPackedValue guards scanRawPackedValueUnchecked against truncated or corrupt input.
func scanRawPackedValue(buf []byte, offset int) (v RawValue, next int, skip bool, err Error) {
	defer func() {
		if recover() != nil {
			v, next, skip, err = RawValue{}, offset, false, newError(types.PARSE_ERROR)
		}
	}()

	v, next, skip, err = scanRawPackedValueUnchecked(buf, offset)
	if err == nil && next > len(buf) {
		return RawValue{}, offset, false, newError(types.PARSE_ERROR)
	}
	return v, next, skip, err
}

func scanRawPackedValueUnchecked(buf []byte, offset int) (RawValue, int, bool, Error) {
	if offset >= len(buf) {
		return RawValue{}, offset, false, newError(types.PARSE_ERROR)
	}

	start := offset
	theType := buf[offset]
	offset++

	switch theType {
	case 0xc0:
		return RawValue{particleType: ParticleType.NULL, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xc2, 0xc3:
		return RawValue{particleType: ParticleType.BOOL, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xca:
		offset += 4
		return RawValue{particleType: ParticleType.FLOAT, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xcb:
		offset += 8
		return RawValue{particleType: ParticleType.FLOAT, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xcc:
		offset++
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xcd:
		offset += 2
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xce:
		offset += 4
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xcf:
		offset += 8
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xd0:
		offset++
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xd1:
		offset += 2
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xd2:
		offset += 4
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xd3:
		offset += 8
		return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
	case 0xc4, 0xd9:
		count := int(buf[offset])
		offset++
		return scanPackedBlobValue(buf, start, offset, count)
	case 0xc5, 0xda:
		count := int(Buffer.BytesToUint16(buf, offset))
		offset += 2
		return scanPackedBlobValue(buf, start, offset, count)
	case 0xc6, 0xdb:
		count := int(Buffer.BytesToUint32(buf, offset))
		offset += 4
		return scanPackedBlobValue(buf, start, offset, count)
	case 0xdc:
		count := int(Buffer.BytesToUint16(buf, offset))
		offset += 2
		next, err := skipPackedObjects(buf, offset, count)
		if err != nil {
			return RawValue{}, offset, false, err
		}
		return RawValue{particleType: ParticleType.LIST, buf: buf[start:next], packed: true}, next, false, nil
	case 0xdd:
		count := int(Buffer.BytesToUint32(buf, offset))
		offset += 4
		next, err := skipPackedObjects(buf, offset, count)
		if err != nil {
			return RawValue{}, offset, false, err
		}
		return RawValue{particleType: ParticleType.LIST, buf: buf[start:next], packed: true}, next, false, nil
	case 0xde:
		count := int(Buffer.BytesToUint16(buf, offset))
		offset += 2
		next, err := skipPackedObjects(buf, offset, count*2)
		if err != nil {
			return RawValue{}, offset, false, err
		}
		return RawValue{particleType: ParticleType.MAP, buf: buf[start:next], packed: true}, next, false, nil
	case 0xdf:
		count := int(Buffer.BytesToUint32(buf, offset))
		offset += 4
		next, err := skipPackedObjects(buf, offset, count*2)
		if err != nil {
			return RawValue{}, offset, false, err
		}
		return RawValue{particleType: ParticleType.MAP, buf: buf[start:next], packed: true}, next, false, nil
	case 0xd4:
		return RawValue{}, offset + 2, true, nil
	case 0xd5:
		return RawValue{}, offset + 3, true, nil
	case 0xd6:
		return RawValue{}, offset + 5, true, nil
	case 0xd7:
		return RawValue{}, offset + 9, true, nil
	case 0xd8:
		return RawValue{}, offset + 17, true, nil
	case 0xc7:
		count := int(buf[offset])
		return RawValue{}, offset + 2 + count, true, nil
	case 0xc8:
		count := int(Buffer.BytesToUint16(buf, offset))
		return RawValue{}, offset + 3 + count, true, nil
	case 0xc9:
		count := int(Buffer.BytesToUint32(buf, offset))
		return RawValue{}, offset + 5 + count, true, nil
	default:
		if (theType & 0xe0) == 0xa0 {
			count := int(theType & 0x1f)
			return scanPackedBlobValue(buf, start, offset, count)
		}
		if (theType & 0xf0) == 0x80 {
			count := int(theType & 0x0f)
			next, err := skipPackedObjects(buf, offset, count*2)
			if err != nil {
				return RawValue{}, offset, false, err
			}
			return RawValue{particleType: ParticleType.MAP, buf: buf[start:next], packed: true}, next, false, nil
		}
		if (theType & 0xf0) == 0x90 {
			count := int(theType & 0x0f)
			next, err := skipPackedObjects(buf, offset, count)
			if err != nil {
				return RawValue{}, offset, false, err
			}
			return RawValue{particleType: ParticleType.LIST, buf: buf[start:next], packed: true}, next, false, nil
		}
		if theType < 0x80 || theType >= 0xe0 {
			return RawValue{particleType: ParticleType.INTEGER, buf: buf[start:offset], packed: true}, offset, false, nil
		}
	}

	return RawValue{}, offset, false, newError(types.SERIALIZE_ERROR)
}

func scanPackedBlobValue(buf []byte, start int, offset int, count int) (RawValue, int, bool, Error) {
	if count < 1 || len(buf) < offset+count {
		return RawValue{}, offset, false, newError(types.PARSE_ERROR)
	}

	particleType := int(buf[offset])
	end := offset + count
	switch particleType {
	case ParticleType.STRING, ParticleType.BLOB, ParticleType.HLL, ParticleType.GEOJSON:
		return RawValue{particleType: particleType, buf: buf[start:end], packed: true}, end, false, nil
	default:
		return RawValue{}, end, false, newError(types.PARSE_ERROR, fmt.Sprintf("unsupported packed particle type %d", particleType))
	}
}

func skipPackedObjects(buf []byte, offset int, count int) (int, Error) {
	for i := 0; i < count; i++ {
		_, next, _, err := scanRawPackedValue(buf, offset)
		if err != nil {
			return offset, err
		}
		offset = next
	}
	return offset, nil
}
