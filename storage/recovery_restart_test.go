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
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
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
			t.Errorf("recovery-observation: could not read reopened metadata during compaction: %v", err)
			return
		}

		switch current.ValueType {
		case pb.ValueType_SEGMENT:
			migrated = true
		case pb.ValueType_RAW_FILE:
			if pendingRows == 0 {
				t.Errorf("recovery-claim: reopened RAW_FILE has no pending compaction row")
				return
			}
		default:
			t.Errorf("recovery-observation: unexpected reopened value type %s", current.ValueType)
			return
		}
		if migrated {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !migrated {
		t.Errorf("recovery-observation: segment migration was not observed before the test window ended")
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
			t.Errorf("recovery-observation: could not stat raw source: %v", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !reclaimed {
		t.Errorf("recovery-observation: raw source was not observed reclaimed before the test window ended")
	}
}

func TestRecoveryRestartDefersPendingCompactionUntilCapacityFits(t *testing.T) {
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
	require.NoError(t, first.Put(fitKey, bytes.NewReader(fitValue), 0))

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
	require.Equal(t, 3, recoveryRestartCompactionRows(t, first))

	first.Close()
	current = nil

	second, err := NewStorageWithConfig(config(smallSegmentSize, DefaultCompactThreshold))
	require.NoError(t, err)
	current = second
	second.compactor.Close()

	// The clamped payload threshold still admits a segment-size-minus-one
	// payload, although its encoded key header makes the full record too large.
	sameConfigBoundaryValue := bytes.Repeat([]byte("s"), int(smallSegmentSize-1))
	require.NoError(t, second.Put(sameConfigBoundaryKey, bytes.NewReader(sameConfigBoundaryValue), 0))
	sameConfigMetadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(sameConfigBoundaryKey)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, sameConfigMetadata.ValueType)
	sameConfigRawPath := sameConfigMetadata.RawFilePath
	require.FileExists(t, sameConfigRawPath)

	processed, bytesCopied := second.compactor.CompactFiles(context.Background(), 0)
	require.LessOrEqual(t, processed, 1)
	if processed == 1 {
		require.Equal(t, int64(len(fitValue)), bytesCopied)
	} else {
		require.Zero(t, bytesCopied)
	}

	for _, key := range []string{largeKey, boundaryKey, sameConfigBoundaryKey} {
		metadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(key)))
		require.NoError(t, err)
		require.Equal(t, pb.ValueType_RAW_FILE, metadata.ValueType)
	}
	fitMetadata, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(fitKey)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_SEGMENT, fitMetadata.ValueType)
	require.Equal(t, 3, recoveryRestartCompactionRows(t, second))
	for _, rawPath := range []string{largeRawPath, boundaryRawPath, sameConfigRawPath} {
		require.FileExists(t, rawPath)
	}
	for key, value := range map[string][]byte{
		largeKey:              largeValue,
		boundaryKey:           boundaryValue,
		fitKey:                fitValue,
		sameConfigBoundaryKey: sameConfigBoundaryValue,
	} {
		recoveryRestartAssertValue(t, second, key, value)
	}

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
	if err != nil {
		t.Errorf("recovery-observation: Get after restart failed: %v", err)
		return
	}
	if !found {
		t.Errorf("recovery-claim: Get after restart reported the stored value absent")
		return
	}
	if reader == nil {
		t.Errorf("recovery-observation: Get after restart returned no reader")
		return
	}

	got, readErr := io.ReadAll(reader)
	if closer, ok := reader.(io.Closer); ok {
		if closeErr := closer.Close(); readErr == nil {
			readErr = closeErr
		}
	}
	if readErr != nil {
		t.Errorf("recovery-observation: reading Get result failed: %v", readErr)
		return
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovery-claim: Get after restart returned bytes different from the Put payload")
	}
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
