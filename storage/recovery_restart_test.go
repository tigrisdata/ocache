// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
)

func TestRecoveryRestartPreservesPendingCompaction(t *testing.T) {
	const key = "recovery-restart-pending-compaction"

	diskPath := t.TempDir()
	config := func() *StorageConfig {
		return &StorageConfig{
			DiskPath:            diskPath,
			InlineThreshold:     DefaultInlineThreshold,
			CompactThreshold:    DefaultCompactThreshold,
			SegmentSize:         DefaultSegmentSize,
			MaxDiskUsage:        0,
			TTL:                 0,
			DisableRecompaction: true,
		}
	}

	first, err := NewStorageWithConfig(config())
	require.NoError(t, err)
	t.Cleanup(func() {
		if first != nil {
			first.Close()
		}
	})

	// Keep the pending row in RocksDB until startup recovery runs.
	first.compactor.Close()
	value := bytes.Repeat([]byte("m"), 2*DefaultInlineThreshold)
	recordAndFooterSize := segment.CalculateValueHeaderSize(key) + int64(len(value)) + int64(segment.SegmentFooterSize)
	require.LessOrEqual(t, recordAndFooterSize, int64(DefaultSegmentSize))
	require.NoError(t, first.Put(key, bytes.NewReader(value), 0))

	beforeRestart, err := utils.GetMetadata(first.meta, string(keys.MakeMetadataKey(key)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, beforeRestart.ValueType)
	rawPath := beforeRestart.RawFilePath
	require.NotEmpty(t, rawPath)
	require.FileExists(t, rawPath)
	require.Equal(t, 1, recoveryRestartCompactionRows(t, first))

	first.Close()
	first = nil

	second, err := NewStorageWithConfig(config())
	require.NoError(t, err)
	t.Cleanup(func() {
		if second != nil {
			second.Close()
		}
	})

	recoveryRestartAssertValue(t, second, key, value)

	// A successful compaction publishes SEGMENT metadata and deletes the
	// compaction row in one RocksDB batch. Read the index first, then metadata:
	// an empty index followed by RAW_FILE metadata is a stable missing-work
	// counterexample, not a slow worker observation.
	migrated := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pendingRows := recoveryRestartCompactionRows(t, second)
		current, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(key)))
		if err != nil {
			t.Errorf("could not read reopened metadata during compaction: %v", err)
			return
		}

		switch current.ValueType {
		case pb.ValueType_SEGMENT:
			migrated = true
		case pb.ValueType_RAW_FILE:
			require.NotZero(t, pendingRows)
		default:
			require.Equal(t, pb.ValueType_SEGMENT, current.ValueType)
			return
		}
		if migrated {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !migrated {
		t.Errorf("segment migration was not observed before the test window ended")
		return
	}

	recoveryRestartAssertValue(t, second, key, value)

	// Compaction publishes the segment before the deletion queue reclaims the
	// old raw source. Waiting observes progress; a timeout is inconclusive, not
	// evidence that recovery erased work.
	reclaimed := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, err := os.Stat(rawPath)
		if os.IsNotExist(err) {
			reclaimed = true
			break
		}
		if err != nil {
			t.Errorf("could not stat raw source: %v", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !reclaimed {
		t.Errorf("raw source was not observed reclaimed before the test window ended")
	}
}

func TestRecoveryRestartDefersUnfitPendingCompaction(t *testing.T) {
	const smallSegmentSize = int64(256 * 1024)
	const largeSegmentSize = int64(4 * 1024 * 1024)
	const largeCompactThreshold = int64(2 * 1024 * 1024)

	const largeKey = "recovery-capacity-shrunk-pending"
	const boundaryKey = "recovery-capacity-header-pending"
	const fitKey = "recovery-capacity-fitting"
	const sameConfigBoundaryKey = "recovery-capacity-same-config"

	diskPath := t.TempDir()
	config := func(segmentSize, compactThreshold int64) *StorageConfig {
		return &StorageConfig{
			DiskPath:            diskPath,
			InlineThreshold:     DefaultInlineThreshold,
			CompactThreshold:    compactThreshold,
			SegmentSize:         segmentSize,
			MaxDiskUsage:        0,
			TTL:                 0,
			DisableRecompaction: true,
			CompactionThreads:   1,
		}
	}

	var current *Storage
	t.Cleanup(func() {
		if current != nil {
			current.Close()
		}
	})

	first, err := NewStorageWithConfig(config(largeSegmentSize, largeCompactThreshold))
	require.NoError(t, err)
	current = first
	first.compactor.Close()

	largeValue := bytes.Repeat([]byte("l"), 512*1024)
	boundaryValue := bytes.Repeat([]byte("b"), int(smallSegmentSize-1))
	fitValue := bytes.Repeat([]byte("f"), 2*DefaultInlineThreshold)
	require.NoError(t, first.Put(largeKey, bytes.NewReader(largeValue), 0))
	require.NoError(t, first.Put(boundaryKey, bytes.NewReader(boundaryValue), 0))

	largeMetadata, err := utils.GetMetadata(first.meta, string(keys.MakeMetadataKey(largeKey)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, largeMetadata.ValueType)
	largeRawPath := largeMetadata.RawFilePath
	require.FileExists(t, largeRawPath)
	boundaryMetadata, err := utils.GetMetadata(first.meta, string(keys.MakeMetadataKey(boundaryKey)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, boundaryMetadata.ValueType)
	boundaryRawPath := boundaryMetadata.RawFilePath
	require.FileExists(t, boundaryRawPath)
	require.Equal(t, 2, recoveryRestartCompactionRows(t, first))

	first.Close()
	current = nil

	second, err := NewStorageWithConfig(config(smallSegmentSize, DefaultCompactThreshold))
	require.NoError(t, err)
	current = second
	second.compactor.Close()

	// Add the fitting row only after the reopened compactor is stopped. The
	// worker may scan the unfit recovered rows during construction, but cannot
	// win the manual-processing assertion for this new row.
	require.NoError(t, second.Put(fitKey, bytes.NewReader(fitValue), 0))

	// This payload is below the clamped threshold and its encoded record fits,
	// but the footer would exceed the configured segment capacity.
	sameConfigHeaderSize := segment.CalculateValueHeaderSize(sameConfigBoundaryKey)
	sameConfigValueSize := smallSegmentSize - int64(segment.SegmentFooterSize) - sameConfigHeaderSize + 1
	sameConfigBoundaryValue := bytes.Repeat([]byte("s"), int(sameConfigValueSize))
	sameConfigRecordSize := sameConfigHeaderSize + int64(len(sameConfigBoundaryValue))
	require.LessOrEqual(t, sameConfigRecordSize, smallSegmentSize)
	require.Greater(t, sameConfigRecordSize+int64(segment.SegmentFooterSize), smallSegmentSize)
	require.NoError(t, second.Put(sameConfigBoundaryKey, bytes.NewReader(sameConfigBoundaryValue), 0))
	sameConfigMetadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(sameConfigBoundaryKey)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, sameConfigMetadata.ValueType)
	sameConfigRawPath := sameConfigMetadata.RawFilePath
	require.FileExists(t, sameConfigRawPath)
	require.Equal(t, 4, recoveryRestartCompactionRows(t, second))

	processed, bytesCopied := second.compactor.CompactFiles(context.Background(), 0)
	assert.Equal(t, 1, processed, "only the record that fits including its footer should be compacted")
	assert.Equal(t, int64(len(fitValue)), bytesCopied)

	for _, key := range []string{largeKey, boundaryKey, sameConfigBoundaryKey} {
		metadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(key)))
		require.NoError(t, err)
		assert.Equal(t, pb.ValueType_RAW_FILE, metadata.ValueType)
	}
	fitMetadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(fitKey)))
	require.NoError(t, err)
	assert.Equal(t, pb.ValueType_SEGMENT, fitMetadata.ValueType)
	assert.Equal(t, 3, recoveryRestartCompactionRows(t, second))
	for _, rawPath := range []string{largeRawPath, boundaryRawPath, sameConfigRawPath} {
		assert.FileExists(t, rawPath)
	}
	for key, value := range map[string][]byte{
		largeKey:              largeValue,
		boundaryKey:           boundaryValue,
		fitKey:                fitValue,
		sameConfigBoundaryKey: sameConfigBoundaryValue,
	} {
		recoveryRestartAssertValue(t, second, key, value)
	}

	// Finalize every segment produced by this pass so the footer's physical
	// bytes are included in the configured-capacity assertion below.
	segmentPaths := map[string]struct{}{fitMetadata.SegmentPath: {}}
	for _, key := range []string{largeKey, boundaryKey, sameConfigBoundaryKey} {
		metadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(key)))
		require.NoError(t, err)
		if metadata.ValueType == pb.ValueType_SEGMENT {
			segmentPaths[metadata.SegmentPath] = struct{}{}
		}
	}
	for path := range segmentPaths {
		seg := second.segmentManager.GetSegmentByPath(path)
		require.NotNil(t, seg)
		require.NoError(t, second.segmentManager.FinalizeSegment(seg))
	}

	// Finalization can grow a physical file past its preallocated size, so
	// check the on-disk limit after closing storage.
	second.Close()
	second = nil
	current = nil

	segmentDir := filepath.Join(diskPath, "segments")
	segmentFiles, err := os.ReadDir(segmentDir)
	require.NoError(t, err)
	for _, entry := range segmentFiles {
		info, err := entry.Info()
		require.NoError(t, err)
		require.LessOrEqual(t, info.Size(), smallSegmentSize, "segment %s exceeds configured capacity", entry.Name())
	}
}

func recoveryRestartAssertValue(t *testing.T, s *Storage, key string, want []byte) {
	t.Helper()

	reader, found, err := s.Get(key, 0, 0)
	require.NoError(t, err)
	if !found {
		require.True(t, found)
		return
	}
	if reader == nil {
		require.NotNil(t, reader)
		return
	}

	got, readErr := io.ReadAll(reader)
	if closer, ok := reader.(io.Closer); ok {
		if closeErr := closer.Close(); readErr == nil {
			readErr = closeErr
		}
	}
	if readErr != nil {
		require.NoError(t, readErr)
		return
	}
	require.Equal(t, want, got)
}

func recoveryRestartCompactionRows(t testing.TB, s *Storage) int {
	t.Helper()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	count := 0
	prefix := []byte(keys.CompactionIndexPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	return count
}
