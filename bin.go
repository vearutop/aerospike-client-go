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
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

const (
	// maxBinNameLength is the maximum bin name length.
	maxBinNameLength = 15
)

// BinMap is used to define a map of bin names to values.
type BinMap map[string]any

// BinWriter writes bins directly into the command buffer using predefined
// Aerospike particle types.
type BinWriter struct {
	cmd       *baseCommand
	operation OperationType
}

// BinEncoder is the low-allocation write path for encoded bin APIs.
//
// WriteBins is called once to emit bins into the command buffer.
type BinEncoder interface {
	WriteBins(w BinWriter) Error
}

// BinSizeHint optionally provides an initial byte-capacity hint for BinEncoder
// writes. The command buffer may still grow beyond this value if needed.
type BinSizeHint interface {
	EncodedBinsSizeHint() int
}

// BinHeaderReceiver optionally receives record metadata when using GetDecodedBins.
type BinHeaderReceiver interface {
	SetHeader(generation uint32, expiration uint32)
}

// RawValue is a view of a server bin value backed by the command buffer.
//
// The underlying bytes are only valid during the GetDecodedBins callback. Copy them if
// they need to outlive the call.
type RawValue struct {
	particleType int
	buf          []byte
	packed       bool
}

// ParticleType returns the Aerospike particle type of the value.
func (rbv RawValue) ParticleType() int {
	return rbv.particleType
}

// Bytes returns the raw payload bytes for blob-like values.
//
// The returned slice aliases the command buffer or packed container buffer and
// is only valid during the callback.
func (rbv RawValue) Bytes() []byte {
	if rbv.packed {
		payload, ok := rbv.packedBytesPayload()
		if !ok {
			return nil
		}
		return payload
	}
	return rbv.buf
}

// Int64 decodes integer particles without extra unpacking.
func (rbv RawValue) Int64() (int64, bool) {
	if rbv.packed {
		return rbv.packedInt64()
	}
	if rbv.particleType != ParticleType.INTEGER {
		return 0, false
	}

	return Buffer.VarBytesToInt64(rbv.buf, 0, len(rbv.buf)), true
}

// Uint64 decodes unsigned integer particles without extra unpacking.
func (rbv RawValue) Uint64() (uint64, bool) {
	if rbv.packed {
		return rbv.packedUint64()
	}
	if rbv.particleType != ParticleType.INTEGER {
		return 0, false
	}

	v := Buffer.VarBytesToInt64(rbv.buf, 0, len(rbv.buf))
	if v < 0 {
		return 0, false
	}

	return uint64(v), true
}

// Float32 decodes float particles as float32 when possible.
func (rbv RawValue) Float32() (float32, bool) {
	if rbv.packed {
		return rbv.packedFloat32()
	}
	if rbv.particleType != ParticleType.FLOAT {
		return 0, false
	}
	if len(rbv.buf) == 4 {
		return Buffer.BytesToFloat32(rbv.buf, 0), true
	}
	if len(rbv.buf) == 8 {
		return float32(Buffer.BytesToFloat64(rbv.buf, 0)), true
	}
	return 0, false
}

// Float64 decodes float particles without extra unpacking.
func (rbv RawValue) Float64() (float64, bool) {
	if rbv.packed {
		return rbv.packedFloat64()
	}
	if rbv.particleType != ParticleType.FLOAT {
		return 0, false
	}

	if len(rbv.buf) == 4 {
		return float64(Buffer.BytesToFloat32(rbv.buf, 0)), true
	}
	return Buffer.BytesToFloat64(rbv.buf, 0), true
}

// Bool decodes bool particles without extra unpacking.
func (rbv RawValue) Bool() (bool, bool) {
	if rbv.packed {
		return rbv.packedBool()
	}
	if rbv.particleType != ParticleType.BOOL {
		return false, false
	}

	return Buffer.BytesToBool(rbv.buf, 0, len(rbv.buf)), true
}

// String decodes string particles.
func (rbv RawValue) String() (string, bool) {
	if rbv.packed {
		payload, ok := rbv.packedStringPayload(ParticleType.STRING)
		if !ok {
			return "", false
		}
		return string(payload), true
	}
	if rbv.particleType != ParticleType.STRING {
		return "", false
	}

	return string(rbv.buf), true
}

// IsNull reports whether the value is a null particle.
func (rbv RawValue) IsNull() bool {
	if rbv.packed {
		return len(rbv.buf) == 1 && rbv.buf[0] == 0xc0
	}
	return rbv.particleType == ParticleType.NULL
}

// GeoJSON decodes GeoJSON particles without generic unpacking.
func (rbv RawValue) GeoJSON() (string, bool) {
	if rbv.packed {
		payload, ok := rbv.packedStringPayload(ParticleType.GEOJSON)
		if !ok {
			return "", false
		}
		return string(payload), true
	}
	if rbv.particleType != ParticleType.GEOJSON || len(rbv.buf) < 3 {
		return "", false
	}

	ncells := int(Buffer.BytesToInt16(rbv.buf, 1))
	headerSize := 1 + 2 + (ncells * 8)
	if len(rbv.buf) < headerSize {
		return "", false
	}
	return string(rbv.buf[headerSize:]), true
}

// HLL returns the raw HyperLogLog bytes.
func (rbv RawValue) HLL() ([]byte, bool) {
	if rbv.packed {
		payload, ok := rbv.packedBytesPayloadForType(ParticleType.HLL)
		return payload, ok
	}
	if rbv.particleType != ParticleType.HLL {
		return nil, false
	}
	return rbv.buf, true
}

// ForEachList iterates over list elements without decoding them into a Go slice.
func (rbv RawValue) ForEachList(fn func(RawValue) Error) Error {
	if rbv.particleType != ParticleType.LIST {
		return ErrInvalidObjectType
	}
	return forEachRawList(rbv.buf, fn)
}

// ForEachMap iterates over map entries without decoding them into a Go map.
func (rbv RawValue) ForEachMap(fn func(RawValue, RawValue) Error) Error {
	if rbv.particleType != ParticleType.MAP {
		return ErrInvalidObjectType
	}
	return forEachRawMap(rbv.buf, fn)
}

// BinDecoder receives bins directly from the wire without building a Record
// or BinMap first.
//
// The provided bin name and value bytes alias the command buffer and are only
// valid during the callback.
type BinDecoder interface {
	SetBin(name []byte, value RawValue) Error
}

// Bin encapsulates a field name/value pair.
type Bin struct {
	// Bin name. Current limit is 15 bytes.
	Name string

	// Bin value.
	Value Value
}

// NewBin generates a new Bin instance, specifying bin name and string value.
// For servers configured as "single-bin", enter an empty name.
func NewBin(name string, value any) *Bin {
	return &Bin{
		Name:  name,
		Value: NewValue(value),
	}
}

// String implements Stringer interface.
func (bn *Bin) String() string {
	return bn.Name + ":" + bn.Value.String()
}
