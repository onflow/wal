package wal

import (
	"bytes"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func Test_LogLocation(t *testing.T) {

	dir, err := ioutil.TempDir("", "loglocation")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.RemoveAll(dir))
	}()

	log, err := NewSize(zerolog.Nop(), nil, dir, 32*1024, false)
	require.NoError(t, err)

	defer log.Close()

	data1 := []byte{1, 1, 1, 1}
	data2 := []byte{2, 2, 2, 2}
	data3 := make([]byte, 33*1024) //larger than segment size
	data4 := []byte{3, 3, 3, 3}
	data5 := []byte{6, 6, 6, 6}
	data6 := []byte{7, 7, 7, 7}

	locations, err := log.Log(data1, data2, data3, data4, data5, data6)
	require.NoError(t, err)

	require.Len(t, locations, 6)

	require.Equal(t, locations[0].Segment, 0)
	require.Equal(t, locations[0].Offset, 0)

	require.Equal(t, locations[1].Segment, 0)
	require.Equal(t, locations[1].Offset, recordHeaderSize+len(data1))

	require.Equal(t, locations[2].Segment, 1) // new segment for large data
	require.Equal(t, locations[2].Offset, 0)

	require.Equal(t, locations[3].Segment, 2) // previous filled entire segment, so next one
	require.Equal(t, locations[3].Offset, 0)

	require.Equal(t, locations[4].Segment, 2)
	require.Equal(t, locations[4].Offset, recordHeaderSize+len(data4))

	require.Equal(t, locations[5].Segment, 2)
	require.Equal(t, locations[5].Offset, recordHeaderSize+len(data4)+recordHeaderSize+len(data5))

	requireLogLocation(t, data1, dir, locations[0])
	requireLogLocation(t, data2, dir, locations[1])
	requireLogLocation(t, data3, dir, locations[2])
	requireLogLocation(t, data4, dir, locations[3])
	requireLogLocation(t, data5, dir, locations[4])
	requireLogLocation(t, data6, dir, locations[5])
}

func requireLogLocation(t *testing.T, record []byte, dir string, ll LogLocation) {

	segBytes, err := ioutil.ReadFile(SegmentName(dir, ll.Segment))
	require.NoError(t, err)

	reader := NewReader(bytes.NewBuffer(segBytes[ll.Offset:]))
	require.True(t, reader.Next())
	require.Equal(t, record, reader.Record())
}
