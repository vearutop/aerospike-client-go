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

// BinValueIter allows callers to provide record bins without creating a BinMap
// or allocating Bin objects on the hot path.
//
// Returned Value objects should be concrete aerospike Value implementations
// such as IntegerValue, StringValue, BytesValue, NewListerValue, or
// NewMapperValue.
type BinValueIter interface {
	Len() int
	Bin(i int) (name string, value Value)
}

// BinValueEncoderIter is an optional low-level fast path for PutBinsIter-style
// APIs. It allows callers to expose bin metadata and write predefined value
// types directly into the wire buffer without boxing them into Value
// interfaces on each iteration.
//
// WriteBin must write only the value payload bytes for the bin selected by i.
// EstimateBin must return the same bin name, particle type, and payload size
// that WriteBin will emit.
type BinValueEncoderIter interface {
	BinValueIter
	EstimateBin(i int) (name string, particleType int, valueSize int, err Error)
	WriteBin(i int, cmd BufferEx) (int, Error)
}

// BinHeaderReceiver optionally receives record metadata when using GetBins.
type BinHeaderReceiver interface {
	SetHeader(generation uint32, expiration uint32)
}

// RawBinValue is a view of a server bin value backed by the command buffer.
//
// The underlying bytes are only valid during the GetBins callback. Copy them if
// they need to outlive the call.
type RawBinValue struct {
	particleType int
	buf          []byte
}

// ParticleType returns the Aerospike particle type of the value.
func (rbv RawBinValue) ParticleType() int {
	return rbv.particleType
}

// Bytes returns the raw value bytes as encoded on the wire.
//
// The returned slice aliases the command buffer and is only valid during the
// GetBins callback.
func (rbv RawBinValue) Bytes() []byte {
	return rbv.buf
}

// Int64 decodes integer particles without extra unpacking.
func (rbv RawBinValue) Int64() (int64, bool) {
	if rbv.particleType != ParticleType.INTEGER {
		return 0, false
	}

	return Buffer.VarBytesToInt64(rbv.buf, 0, len(rbv.buf)), true
}

// Float64 decodes float particles without extra unpacking.
func (rbv RawBinValue) Float64() (float64, bool) {
	if rbv.particleType != ParticleType.FLOAT {
		return 0, false
	}

	return Buffer.BytesToFloat64(rbv.buf, 0), true
}

// Bool decodes bool particles without extra unpacking.
func (rbv RawBinValue) Bool() (bool, bool) {
	if rbv.particleType != ParticleType.BOOL {
		return false, false
	}

	return Buffer.BytesToBool(rbv.buf, 0, len(rbv.buf)), true
}

// String decodes string particles.
func (rbv RawBinValue) String() (string, bool) {
	if rbv.particleType != ParticleType.STRING {
		return "", false
	}

	return string(rbv.buf), true
}

// Interface fully decodes the value using the standard Aerospike decoder.
func (rbv RawBinValue) Interface() (any, Error) {
	return bytesToParticle(rbv.particleType, rbv.buf, 0, len(rbv.buf))
}

// RawBinReceiver receives bins directly from the wire without building a Record
// or BinMap first.
//
// The provided bin name and value bytes alias the command buffer and are only
// valid during the callback.
type RawBinReceiver interface {
	SetBin(name []byte, value RawBinValue) Error
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
