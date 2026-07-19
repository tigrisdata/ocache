// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/fd"
)

// leakTestStorage builds a Storage with tiny thresholds so a small payload
// lands as a *large* raw file (> inlineThreshold makes it a RAW_FILE, >
// compactThreshold makes it "large": never compacted, so it is reclaimed only
// by LRU eviction). The background cleaner is disabled so the test drives
// eviction deterministically via evictByIndex(lruEvictionIndex()).
func leakTestStorage(t *testing.T) (*Storage, func()) {
	dir := t.TempDir()
	config := &StorageConfig{
		DiskPath:         dir,
		MaxDiskUsage:     100 * 1024 * 1024,
		InlineThreshold:  1024,     // > 1KB  → raw file
		CompactThreshold: 4 * 1024, // > 4KB  → large (never compacted)
		SegmentSize:      256 * 1024 * 1024,
		// Disable the periodic TTL/LRU cleaner so the test drives eviction via
		// evictByIndex directly. (0 would mean "use default" — a 1-min interval.)
		// The deletion queue's own background ProcessBatch still runs; the read
		// lock the test holds is what gates it, which is exactly what we assert.
		CleanupInterval: 24 * time.Hour,
	}
	s, err := NewStorageWithConfig(config)
	require.NoError(t, err)
	return s, func() { s.Close() }
}

// TestLRUEviction_LockedRawFile_QueuedNotOrphaned reproduces the disk leak where
// LRU eviction of a large raw file that is concurrently being read would drop
// the metadata but never delete the file (fileManager.Remove uses a
// non-blocking TryLock and was silently skipped), orphaning it permanently. The
// fix routes the deletion through the retrying deletion queue instead.
func TestLRUEviction_LockedRawFile_QueuedNotOrphaned(t *testing.T) {
	s, cleanup := leakTestStorage(t)
	defer cleanup()

	key := "locked-large"
	value := bytes.Repeat([]byte("z"), 8*1024) // 8KB > compactThreshold → large raw file
	require.NoError(t, s.Put(key, bytes.NewReader(value), 0))

	rawPath, _ := rawFilePathOf(t, s, key)
	require.FileExists(t, rawPath)

	// Simulate an in-flight read: hold the per-file read lock so the deletion's
	// exclusive TryLock cannot acquire it (exactly what a concurrent Get does).
	readLock := fd.GetFileLockManager().GetFileLock(rawPath)
	readLock.RLock()

	// Evict everything. The metadata entry is removed; the still-locked file
	// must NOT be silently dropped — it must be handed to the deletion queue.
	cleaner := NewCleaner(s, time.Hour, 1)
	evicted := cleaner.evictByIndex(lruEvictionIndex(), 1<<30)
	require.Greater(t, evicted, 0, "the large raw-file key should have been evicted")

	// File survives (it was read-locked) but is now tracked for retry, not lost.
	assert.FileExists(t, rawPath, "read-locked file must not be deleted out from under the reader")
	assert.Greater(t, s.deletionQueue.GetQueueDepth(), int64(0),
		"evicted raw file must be queued for deletion, not orphaned")

	// Once the reader finishes, the queue reclaims the file.
	readLock.RUnlock()
	s.deletionQueue.ProcessBatch()
	assert.NoFileExists(t, rawPath, "deletion queue should reclaim the file after the read lock is released")
	assert.Equal(t, int64(0), s.deletionQueue.GetQueueDepth(), "queue should be drained after successful deletion")
}
