// Copyright 2019 The Prometheus Authors
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

package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	tsdb_errors "github.com/m4ksio/wal/errors"
)

type reader interface {
	Next() bool
	Err() error
	Record() []byte
	Offset() int64
}

type rec struct {
	t recType
	b []byte
}

var readerConstructors = map[string]func(io.Reader) reader{
	"Reader": func(r io.Reader) reader {
		return NewReader(r)
	},
}

var data = make([]byte, 100000)
var testReaderCases = []struct {
	t    []rec
	exp  [][]byte
	fail bool
}{
	// Sequence of valid records.
	{
		t: []rec{
			{recFull, data[0:200]},
			{recFirst, data[200:300]},
			{recLast, data[300:400]},
			{recFirst, data[400:800]},
			{recMiddle, data[800:900]},
			{recPageTerm, make([]byte, pageSize-900-recordHeaderSize*5-1)}, // exactly lines up with page boundary.
			{recLast, data[900:900]},
			{recFirst, data[900:1000]},
			{recMiddle, data[1000:1200]},
			{recMiddle, data[1200:30000]},
			{recMiddle, data[30000:30001]},
			{recMiddle, data[30001:30001]},
			{recLast, data[30001:32000]},
		},
		exp: [][]byte{
			data[0:200],
			data[200:400],
			data[400:900],
			data[900:32000],
		},
	},
	// Exactly at the limit of one page minus the header size
	{
		t: []rec{
			{recFull, data[0 : pageSize-recordHeaderSize]},
		},
		exp: [][]byte{
			data[:pageSize-recordHeaderSize],
		},
	},
	// More than a full page, this exceeds our buffer and can never happen
	// when written by the WAL.
	{
		t: []rec{
			{recFull, data[0 : pageSize+1]},
		},
		fail: true,
	},
	// Two records the together are too big for a page.
	// NB currently the non-live reader succeeds on this. I think this is a bug.
	// but we've seen it in production.
	{
		t: []rec{
			{recFull, data[:pageSize/2]},
			{recFull, data[:pageSize/2]},
		},
		exp: [][]byte{
			data[:pageSize/2],
			data[:pageSize/2],
		},
	},
	// Invalid orders of record types.
	{
		t:    []rec{{recMiddle, data[:200]}},
		fail: true,
	},
	{
		t:    []rec{{recLast, data[:200]}},
		fail: true,
	},
	{
		t: []rec{
			{recFirst, data[:200]},
			{recFull, data[200:400]},
		},
		fail: true,
	},
	{
		t: []rec{
			{recFirst, data[:100]},
			{recMiddle, data[100:200]},
			{recFull, data[200:400]},
		},
		fail: true,
	},
	// Non-zero data after page termination.
	{
		t: []rec{
			{recFull, data[:100]},
			{recPageTerm, append(make([]byte, pageSize-recordHeaderSize-102), 1)},
		},
		exp:  [][]byte{data[:100]},
		fail: true,
	},
}

func encodedRecord(t recType, b []byte) []byte {
	if t == recPageTerm {
		return append([]byte{0}, b...)
	}
	r := make([]byte, recordHeaderSize)
	r[0] = byte(t)
	binary.BigEndian.PutUint16(r[1:], uint16(len(b)))
	binary.BigEndian.PutUint32(r[3:], crc32.Checksum(b, castagnoliTable))
	return append(r, b...)
}

// TestReader feeds the reader a stream of encoded records with different types.
func TestReader(t *testing.T) {
	for name, fn := range readerConstructors {
		for i, c := range testReaderCases {
			t.Run(fmt.Sprintf("%s/%d", name, i), func(t *testing.T) {
				var buf []byte
				for _, r := range c.t {
					buf = append(buf, encodedRecord(r.t, r.b)...)
				}
				r := fn(bytes.NewReader(buf))

				for j := 0; r.Next(); j++ {
					t.Logf("record %d", j)
					rec := r.Record()

					if j >= len(c.exp) {
						t.Fatal("received more records than expected")
					}
					assert.Equal(t, c.exp[j], rec, "Bytes within record did not match expected Bytes")
				}
				if !c.fail && r.Err() != nil {
					t.Fatalf("unexpected error: %s", r.Err())
				}
				if c.fail && r.Err() == nil {
					t.Fatalf("expected error but got none")
				}
			})
		}
	}
}

const fuzzLen = 500

func generateRandomEntries(w *WAL, records chan []byte) error {
	var recs [][]byte
	for i := 0; i < fuzzLen; i++ {
		var sz int64
		switch i % 5 {
		case 0, 1:
			sz = 50
		case 2, 3:
			sz = pageSize
		default:
			sz = pageSize * 8
		}

		rec := make([]byte, rand.Int63n(sz))
		if _, err := rand.Read(rec); err != nil {
			return err
		}

		records <- rec

		// Randomly batch up records.
		recs = append(recs, rec)
		if rand.Intn(4) < 3 {
			if _, err := w.Log(recs...); err != nil {
				return err
			}
			recs = recs[:0]
		}
	}
	_, err := w.Log(recs...)
	return err
}

type multiReadCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func (m *multiReadCloser) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}
func (m *multiReadCloser) Close() error {
	var merr tsdb_errors.MultiError
	for _, closer := range m.closers {
		merr.Add(closer.Close())
	}
	return merr.Err()
}

func allSegments(dir string) (io.ReadCloser, error) {
	seg, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	var readers []io.Reader
	var closers []io.Closer
	for _, r := range seg {
		f, err := os.Open(filepath.Join(dir, r.name))
		if err != nil {
			return nil, err
		}
		readers = append(readers, f)
		closers = append(closers, f)
	}

	return &multiReadCloser{
		reader:  io.MultiReader(readers...),
		closers: closers,
	}, nil
}

func TestReaderFuzz(t *testing.T) {
	for name, fn := range readerConstructors {
		for _, compress := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s,compress=%t", name, compress), func(t *testing.T) {
				dir, err := ioutil.TempDir("", "wal_fuzz_live")
				assert.NoError(t, err)
				defer func() {
					assert.NoError(t, os.RemoveAll(dir))
				}()

				w, err := NewSize(zerolog.Nop(), nil, dir, 128*pageSize, compress)
				assert.NoError(t, err)

				// Buffering required as we're not reading concurrently.
				input := make(chan []byte, fuzzLen)
				err = generateRandomEntries(w, input)
				assert.NoError(t, err)
				close(input)

				err = w.Close()
				assert.NoError(t, err)

				sr, err := allSegments(w.Dir())
				assert.NoError(t, err)
				defer sr.Close()

				reader := fn(sr)
				for expected := range input {
					assert.True(t, reader.Next(), "expected record: %v", reader.Err())
					assert.Equal(t, expected, reader.Record(), "read wrong record")
				}
				assert.True(t, !reader.Next(), "unexpected record")
			})
		}
	}
}

func TestReaderData(t *testing.T) {
	dir := os.Getenv("WALDIR")
	if dir == "" {
		return
	}

	for name, fn := range readerConstructors {
		t.Run(name, func(t *testing.T) {
			w, err := New(zerolog.Nop(), nil, dir, true)
			assert.NoError(t, err)

			sr, err := allSegments(dir)
			assert.NoError(t, err)

			reader := fn(sr)
			for reader.Next() {
			}
			assert.NoError(t, reader.Err())

			err = w.Repair(reader.Err())
			assert.NoError(t, err)
		})
	}
}
