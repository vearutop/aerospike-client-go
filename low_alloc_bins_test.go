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
	as "github.com/aerospike/aerospike-client-go/v8"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

type testBinIter struct {
	count int
	name  string
}

func (t testBinIter) Len() int {
	return 2
}

func (t testBinIter) EstimateBin(i int) (string, int, int, as.Error) {
	switch i {
	case 0:
		return "count", ParticleType.INTEGER, 8, nil
	case 1:
		return "name", ParticleType.STRING, len(t.name), nil
	default:
		return "", 0, 0, as.ErrInvalidObjectType
	}
}

func (t testBinIter) WriteBin(i int, cmd as.BufferEx) (int, as.Error) {
	switch i {
	case 0:
		return cmd.WriteInt64(int64(t.count)), nil
	case 1:
		return cmd.WriteString(t.name)
	default:
		return 0, as.ErrInvalidObjectType
	}
}

type testRawBinReceiver struct {
	generation uint32
	expiration uint32
	count      int64
	name       string
}

func (t *testRawBinReceiver) SetHeader(generation uint32, expiration uint32) {
	t.generation = generation
	t.expiration = expiration
}

func (t *testRawBinReceiver) SetBin(name []byte, value as.RawBinValue) as.Error {
	switch string(name) {
	case "count":
		v, ok := value.Int64()
		if !ok {
			return as.ErrInvalidObjectType
		}
		t.count = v
	case "name":
		v, ok := value.String()
		if !ok {
			return as.ErrInvalidObjectType
		}
		t.name = v
	}

	return nil
}

var _ = gg.Describe("Low allocation bin APIs", func() {
	gg.It("writes bins from an iterator", func() {
		ns := *namespace
		set := randString(50)
		key, err := as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		err = client.PutBinsIter(nil, key, testBinIter{count: 42, name: "fast"})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["count"]).To(gm.Equal(42))
		gm.Expect(rec.Bins["name"]).To(gm.Equal("fast"))
	})

	gg.It("streams bins into a receiver without constructing a record", func() {
		ns := *namespace
		set := randString(50)
		key, err := as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		err = client.PutBins(nil, key,
			as.NewBin("count", 7),
			as.NewBin("name", "streamed"),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		var receiver testRawBinReceiver
		err = client.GetBins(nil, key, &receiver, "count", "name")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(receiver.generation).To(gm.BeNumerically(">=", 1))
		gm.Expect(receiver.count).To(gm.Equal(int64(7)))
		gm.Expect(receiver.name).To(gm.Equal("streamed"))
	})
})
