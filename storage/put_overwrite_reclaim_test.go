// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"os"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// Overwriting a key whose current value lives in a segment must credit the
// replaced bytes to that segment's delete index. putLow replaces the metadata
// row in place and reads only the previous row's ValueLength (for size
// accounting), so without staging the replaced value the segment bytes go dead
// with no `!delete:segment/` record — invisible to the recompactor forever, and
// unreclaimable physical disk.
func TestPutOverwrite_CreditsReplacedSegmentBytes(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "overwritten_key"
	segmentPath := "/data/segments/segment1.seg"
	valueSize := int64(4096)

	// Simulate the post-compaction state: the key's value lives in a segment.
	valueMsg := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   segmentPath,
		SegmentOffset: 0,
		ValueLength:   valueSize,
	}
	data, err := proto.Marshal(valueMsg)
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data))

	// Overwrite the key. The segment copy is now dead.
	require.NoError(t, s.Put(key, bytes.NewReader([]byte("new")), 0))

	entries, deadBytes, err := s.GetDeleteIndexStats(segmentPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), entries, "overwrite did not credit the replaced segment entry")
	require.Equal(t, valueSize, deadBytes, "overwrite did not credit the replaced segment bytes")
}

// The raw-file analogue: overwriting a RAW_FILE-backed key must queue the
// replaced file for deletion, otherwise files/ accumulates orphans.
func TestPutOverwrite_ReclaimsReplacedRawFile(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "raw_key"
	big := bytes.Repeat([]byte("a"), 1024) // > inlineThreshold -> RAW_FILE

	require.NoError(t, s.Put(key, bytes.NewReader(big), 0))
	first := rawFilePath(t, s, key)

	require.NoError(t, s.Put(key, bytes.NewReader(big), 0))
	second := rawFilePath(t, s, key)
	require.NotEqual(t, first, second, "expected a fresh raw file per Put")

	require.Eventually(t, func() bool {
		_, err := os.Stat(first)
		return os.IsNotExist(err)
	}, 5*time.Second, 50*time.Millisecond, "replaced raw file was never reclaimed")
}

func rawFilePath(t *testing.T, s *Storage, key string) string {
	t.Helper()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := s.meta.Handle().Get(ro, keys.MakeMetadataKey(key))
	require.NoError(t, err)
	defer slice.Free()
	require.True(t, slice.Exists())
	var vm pb.ValueMessage
	require.NoError(t, proto.Unmarshal(slice.Data(), &vm))
	return vm.RawFilePath
}
