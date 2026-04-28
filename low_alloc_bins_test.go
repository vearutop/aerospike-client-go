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
	"fmt"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

type testBinIter struct {
	count int
	name  string
}

func (t *testBinIter) InitialBufferSize() int {
	return 2*8 + len("count") + len("name") + len(t.name)
}

func (t *testBinIter) WriteBins(w as.BinWriter) as.Error {
	if err := w.WriteInt("count", t.count); err != nil {
		return err
	}
	return w.WriteString("name", t.name)
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

		iter := testBinIter{count: 42, name: "fast"}
		err = client.PutEncodedBins(nil, key, &iter)
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

	gg.It("supports concurrent low allocation read and write paths", func() {
		ns := *namespace
		set := randString(50)

		const workers = 8
		const iterations = 16

		errCh := make(chan error, workers*iterations)
		var wg sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			worker := worker
			wg.Add(1)

			go func() {
				defer gg.GinkgoRecover()
				defer wg.Done()

				for i := 0; i < iterations; i++ {
					expectedCount := worker*100 + i
					expectedName := fmt.Sprintf("fast-%d-%d", worker, i)

					key, err := as.NewKey(ns, set, fmt.Sprintf("lowalloc-%d-%d", worker, i))
					if err != nil {
						errCh <- err
						return
					}

					iter := testBinIter{count: expectedCount, name: expectedName}
					if err := client.PutEncodedBins(nil, key, &iter); err != nil {
						errCh <- err
						return
					}

					var receiver testRawBinReceiver
					if err := client.GetBins(nil, key, &receiver, "count", "name"); err != nil {
						errCh <- err
						return
					}

					if receiver.count != int64(expectedCount) {
						errCh <- fmt.Errorf("count mismatch: got %d want %d", receiver.count, expectedCount)
						return
					}

					if receiver.name != expectedName {
						errCh <- fmt.Errorf("name mismatch: got %q want %q", receiver.name, expectedName)
						return
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})
})
