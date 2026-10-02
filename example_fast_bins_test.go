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

package aerospike_test

import (
	"bytes"
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

type exampleFastRecord struct {
	Count int
	Name  string
	Env   string
}

var (
	exampleCountBin = []byte("count")
	exampleNameBin  = []byte("name")
	exampleTagsBin  = []byte("tags")
)

func (e *exampleFastRecord) EncodedBinsSizeHint() int {
	// This is an initial capacity hint for the encoded bin payloads.
	// It helps the client avoid buffer growth, but it does not need to be exact.
	// The estimate below is:
	// - one small per-bin header per encoded bin
	// - bin name bytes for "count", "name", and "tags"
	// - string payload bytes for Name
	// - packed map payload bytes for tags
	const perBinOverhead = 8

	tagsSize, _ := as.PackMap(nil, exampleStringMap{"env": e.Env})
	return 3*perBinOverhead + len("count") + len("name") + len("tags") + len(e.Name) + tagsSize
}

// Pointer receivers matter here because the fast path is interface-based.
// Passing &record to PutEncodedBins/GetDecodedBins avoids boxing a full struct
// value into the interface. In practice that tends to reduce copying and can
// avoid an extra allocation on the hot path.
func (e *exampleFastRecord) WriteBins(w as.BinWriter) as.Error {
	if err := w.WriteInt("count", e.Count); err != nil {
		return err
	}
	if err := w.WriteString("name", e.Name); err != nil {
		return err
	}
	return w.WriteMap("tags", exampleStringMap{"env": e.Env})
}

func (e *exampleFastRecord) SetBin(name []byte, value as.RawValue) as.Error {
	// Compare bin names as bytes to stay on the low-allocation path.
	// Converting name to string or rebuilding []byte("count") on each callback
	// does extra work that a hot decoder can avoid.
	switch {
	case bytes.Equal(name, exampleCountBin):
		v, ok := value.Int64()
		if !ok {
			return as.ErrInvalidObjectType
		}
		e.Count = int(v)
	case bytes.Equal(name, exampleNameBin):
		v, ok := value.String()
		if !ok {
			return as.ErrInvalidObjectType
		}
		e.Name = v
	case bytes.Equal(name, exampleTagsBin):
		return value.ForEachMap(func(key, val as.RawValue) as.Error {
			k, ok := key.String()
			if !ok {
				return as.ErrInvalidObjectType
			}
			if k != "env" {
				return nil
			}
			v, ok := val.String()
			if !ok {
				return as.ErrInvalidObjectType
			}
			e.Env = v
			return nil
		})
	}

	return nil
}

type exampleStringMap map[string]string

func (m exampleStringMap) PackMap(buf as.BufferEx) (int, error) {
	size := 0
	for key, value := range m {
		n, err := as.PackString(buf, key)
		size += n
		if err != nil {
			return size, err
		}

		n, err = as.PackString(buf, value)
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (m exampleStringMap) Len() int {
	return len(m)
}

func ExampleClient_PutEncodedBins() {
	if client == nil || namespace == nil {
		return
	}

	key, err := as.NewKey(*namespace, "test", "encoded-bins-example")
	if err != nil {
		log.Fatal(err)
	}

	if _, err = client.Delete(nil, key); err != nil {
		log.Fatal(err)
	}

	record := exampleFastRecord{
		Count: 42,
		Name:  "fast",
		Env:   "prod",
	}
	if err = client.PutEncodedBins(nil, key, &record); err != nil {
		log.Fatal(err)
	}

	var decoded exampleFastRecord
	if err = client.GetDecodedBins(nil, key, &decoded, "count", "name", "tags"); err != nil {
		log.Fatal(err)
	}

	fmt.Println(decoded.Count, decoded.Name, decoded.Env)
}
