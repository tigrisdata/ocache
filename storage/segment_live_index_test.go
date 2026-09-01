// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"hash/crc32"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/compaction"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"google.golang.org/protobuf/proto"
)

func createIndexedSegmentValue(t *testing.T, s *Storage, key string, payload []byte, expiry int64) (*segment.Segment, *pb.ValueMessage) {
	t.Helper()
	seg, err := s.segmentManager.AcquireOpenSegmentWithReservation("segment-live-index-test", 0)
	require.NoError(t, err)
	checksum := crc32.ChecksumIEEE(payload)
	writeMessage := &pb.ValueMessage{
		ValueType:   pb.ValueType_SEGMENT,
		ValueLength: int64(len(payload)),
		Checksum:    checksum,
	}
	offset, err := seg.WriteEntry(key, bytes.NewReader(payload), writeMessage)
	require.NoError(t, err)

	value := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		ValueLength:   int64(len(payload)),
		Checksum:      checksum,
		Expiry:        expiry,
		SegmentPath:   seg.Path(),
		SegmentOffset: offset,
	}
	valueBytes, err := proto.Marshal(value)
	require.NoError(t, err)
	batch := grocksdb.NewWriteBatch()
	batch.Put(keys.MakeMetadataKey(key), valueBytes)
	indexValue, err := keys.EncodeSegmentLiveIndexEntry(keys.SegmentLiveIndexEntry{
		Key:           key,
		ValueLength:   int64(len(payload)),
		HeaderSize:    segment.CalculateValueHeaderSize(key),
		Checksum:      checksum,
		HeaderVersion: segment.CurrentValueHeaderVersion,
	})
	require.NoError(t, err)
	batch.Put(keys.MakeSegmentLiveIndexKey(seg.Path(), offset), indexValue)
	wo := grocksdb.NewDefaultWriteOptions()
	require.NoError(t, s.meta.Handle().Write(wo, batch))
	wo.Destroy()
	batch.Destroy()
	require.NoError(t, s.segmentManager.FinalizeSegment(seg))
	require.NoError(t, compaction.MarkSegmentLiveIndexComplete(s.meta, seg))
	return seg, value
}

func segmentLiveIndexRowExists(t *testing.T, s *Storage, segmentPath string, offset int64) bool {
	t.Helper()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := s.meta.Handle().Get(ro, keys.MakeSegmentLiveIndexKey(segmentPath, offset))
	require.NoError(t, err)
	exists := slice.Exists()
	slice.Free()
	return exists
}

func TestSegmentLiveIndex_MutationPathsRemoveRowsAtomically(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
		defer cleanup()
		seg, value := createIndexedSegmentValue(t, s, "delete-key", []byte("delete payload"), 0)
		require.True(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))

		require.NoError(t, s.DeleteKey("delete-key"))
		require.False(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))
		deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(seg.Path())
		require.NoError(t, err)
		require.Equal(t, int64(1), deletedEntries)
		require.Equal(t, value.ValueLength, deletedBytes)
	})

	t.Run("overwrite", func(t *testing.T) {
		s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
		defer cleanup()
		seg, value := createIndexedSegmentValue(t, s, "overwrite-key", []byte("old payload"), 0)
		require.True(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))

		replacement := &pb.ValueMessage{ValueType: pb.ValueType_INLINE, ValueLength: 3, Data: []byte("new")}
		replacementBytes, err := proto.Marshal(replacement)
		require.NoError(t, err)
		_, err = s.putLow("overwrite-key", replacementBytes, "", replacement.ValueLength)
		require.NoError(t, err)
		require.False(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))
		deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(seg.Path())
		require.NoError(t, err)
		require.Equal(t, int64(1), deletedEntries)
		require.Equal(t, value.ValueLength, deletedBytes)
	})

	t.Run("ttl cleanup", func(t *testing.T) {
		s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
		defer cleanup()
		seg, value := createIndexedSegmentValue(t, s, "expired-key", []byte("expired payload"), time.Now().Add(-time.Hour).Unix())
		require.True(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))

		s.cleaner.cleanupExpiredKeys()
		require.False(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))
		deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(seg.Path())
		require.NoError(t, err)
		require.Equal(t, int64(1), deletedEntries)
		require.Equal(t, value.ValueLength, deletedBytes)
	})

	t.Run("eviction", func(t *testing.T) {
		s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
		defer cleanup()
		seg, value := createIndexedSegmentValue(t, s, "evicted-key", []byte("evicted payload"), 0)
		now := time.Now()
		accessKey := keys.MakeBucketedAccessKey("evicted-key", now)
		wo := grocksdb.NewDefaultWriteOptions()
		batch := grocksdb.NewWriteBatch()
		batch.Put(accessKey, []byte{})
		batch.Put(keys.MakeBucketedAccessIndexKey("evicted-key"), accessKey)
		require.NoError(t, s.meta.Handle().Write(wo, batch))
		batch.Destroy()
		wo.Destroy()

		require.Equal(t, 1, s.cleaner.evictByIndex(lruEvictionIndex(), value.ValueLength))
		require.False(t, segmentLiveIndexRowExists(t, s, seg.Path(), value.SegmentOffset))
		deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(seg.Path())
		require.NoError(t, err)
		require.Equal(t, int64(1), deletedEntries)
		require.Equal(t, value.ValueLength, deletedBytes)
	})
}
