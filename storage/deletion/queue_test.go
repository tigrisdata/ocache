// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

func setupTestQueue(t *testing.T) (*Queue, func()) {
	tmpDir := t.TempDir()
	meta, err := metadata.NewMetaDB(tmpDir, 0, nil, nil) // nil merge operator for tests
	require.NoError(t, err)

	config := Config{
		BatchSize:       10, // Small batch size for testing
		ProcessInterval: 100 * time.Millisecond,
		PruneAge:        1 * time.Hour,
	}

	queue := NewQueue(meta, config)

	cleanup := func() {
		queue.Stop()
		meta.Close()
	}

	return queue, cleanup
}

func TestQueue_AddAndProcess(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create test files
	tmpDir := t.TempDir()
	testFiles := []string{
		filepath.Join(tmpDir, "file1.txt"),
		filepath.Join(tmpDir, "file2.txt"),
		filepath.Join(tmpDir, "file3.txt"),
	}

	for _, file := range testFiles {
		err := os.WriteFile(file, []byte("test"), 0o644)
		require.NoError(t, err)
	}

	// Add files to queue
	for _, file := range testFiles {
		err := queue.Add(file)
		require.NoError(t, err)
	}

	// Process batch
	queue.ProcessBatch()

	// Verify files are deleted
	for _, file := range testFiles {
		_, err := os.Stat(file)
		require.True(t, os.IsNotExist(err), "file should be deleted: %s", file)
	}
}

func TestQueue_Deduplication(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create a test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "duplicate.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Add the same file multiple times
	for i := 0; i < 5; i++ {
		err := queue.Add(testFile)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Process batch - should only delete once
	queue.ProcessBatch()

	// Verify file is deleted
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted")

	// Check that processed count is 1, not 5, and all duplicate queue rows
	// were retired with the one successful deletion.
	require.Equal(t, int64(1), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_PreservesLaterQueueEntry(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	f := filepath.Join(t.TempDir(), "later-entry.bin")
	require.NoError(t, os.WriteFile(f, []byte("first"), 0o644))
	for i := 0; i < 5; i++ {
		require.NoError(t, queue.Add(f))
		time.Sleep(time.Millisecond)
	}

	// A later-generation queue key is outside this pass because it is not due.
	// The batch must delete only keys it observed, not every key for the path.
	futureKey := keys.MakeDeletionQueueKey(time.Now().Add(24*time.Hour).UnixNano(), f)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Put(wo, futureKey, []byte{0x01}))

	queue.ProcessBatch()

	require.NoFileExists(t, f)
	require.Equal(t, int64(1), queue.GetQueueDepth(), "a later queue generation must survive the earlier batch")
}

func TestQueue_ProcessBatch_DeduplicatesPastBatchBoundary(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	queue.config.BatchSize = 2
	queue.config.RetryDelay = time.Hour

	tmpDir := t.TempDir()
	stuck := filepath.Join(tmpDir, "boundary-stuck.txt")
	other := filepath.Join(tmpDir, "boundary-other.txt")
	tail := filepath.Join(tmpDir, "boundary-tail.txt")
	require.NoError(t, os.WriteFile(stuck, []byte("stuck"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))
	require.NoError(t, os.WriteFile(tail, []byte("tail"), 0o644))

	// Put a duplicate for the locked path after an unselected distinct path
	// and after the second distinct-path boundary. The first processing pass
	// must still coalesce it with the selected group, or it will be retried
	// immediately on the next tick.
	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, stuck), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, tail), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+3, stuck), []byte{0x01})

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	lock := fd.GetFileLockManager().GetFileLock(stuck)
	lock.RLock()
	defer lock.RUnlock()

	queue.ProcessBatch()

	require.FileExists(t, stuck)
	require.NoFileExists(t, other)
	require.FileExists(t, tail, "the distinct path beyond the batch remains queued")
	require.Equal(t, int64(1), queue.failed)
	require.Equal(t, int64(3), queue.GetQueueDepth(), "the unselected tail and duplicate await the next bounded scan")

	// The delayed retry is not due yet, so another worker tick can retire the
	// duplicate and process the unselected tail without making a second attempt
	// on the locked path.
	queue.ProcessBatch()
	require.NoFileExists(t, tail)
	require.Equal(t, int64(1), queue.failed)
	require.Equal(t, int64(1), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_DeduplicatesSuccessfulPastBatchBoundary(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 2

	tmpDir := t.TempDir()
	first := filepath.Join(tmpDir, "successful-first.txt")
	second := filepath.Join(tmpDir, "successful-second.txt")
	third := filepath.Join(tmpDir, "successful-third.txt")
	for _, path := range []string{first, second, third} {
		require.NoError(t, os.WriteFile(path, []byte(path), 0o644))
	}
	firstGeneration := fileGeneration(first)
	secondGeneration := fileGeneration(second)
	thirdGeneration := fileGeneration(third)

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, first), encodeDeletionQueueValue(firstGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, second), encodeDeletionQueueValue(secondGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, third), encodeDeletionQueueValue(thirdGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+3, first), encodeDeletionQueueValue(firstGeneration))
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()
	queue.ProcessBatch()

	require.NoFileExists(t, first)
	require.NoFileExists(t, second)
	require.NoFileExists(t, third)
	require.Equal(t, int64(3), queue.processed, "the duplicate after the batch boundary must not trigger another deletion attempt")
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_DoesNotRetireLegacyEntryPastBoundary(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1

	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "legacy-boundary-path.bin")
	other := filepath.Join(filesDir, "legacy-boundary-other.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, path), []byte{0x01})
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.NoError(t, os.WriteFile(path, []byte("recreated"), 0o644))

	queue.ProcessBatch()

	require.FileExists(t, path, "the ordered boundary must exclude a later legacy queue entry")
	require.Equal(t, int64(1), queue.GetQueueDepth())

	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_UsesNewestLegacyLifecycle(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "mixed-generation-path.bin")
	require.NoError(t, os.WriteFile(path, []byte("current"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), encodeDeletionQueueValue("stale-generation"))
	// This legacy row is newer than the known row. It represents the newest
	// lifecycle and must not be discarded just because an older row is known.
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, path), []byte{0x01})
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()

	require.NoFileExists(t, path, "the newest legacy request must still be processed")
	require.Equal(t, int64(1), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_PreservesMissingPathAddPastBoundary(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1

	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "missing-generation-path.bin")
	other := filepath.Join(filesDir, "missing-generation-other.bin")
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))

	// Both path entries are queued while the pathname is absent. The later Add
	// is beyond the first distinct-path boundary, so it must survive the first
	// success watermark for the earlier missing-path lifecycle.
	require.NoError(t, queue.Add(path))
	require.NoError(t, queue.Add(other))
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()

	require.NoError(t, os.WriteFile(path, []byte("recreated"), 0o644))
	queue.ProcessBatch()

	require.FileExists(t, path, "a later missing-path Add must not be retired as an old duplicate")
	require.Equal(t, int64(1), queue.GetQueueDepth())

	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_ProcessBatch_PreservesNewGenerationPastBoundary(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1

	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "generation-path.bin")
	other := filepath.Join(filesDir, "generation-other.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	oldGeneration := fileGeneration(path)
	// Create the replacement while the old pathname still exists. This keeps
	// the two filesystem identities distinct even when the filesystem reuses
	// the inode of a recently removed file.
	replacement := filepath.Join(filesDir, "generation-replacement.bin")
	require.NoError(t, os.WriteFile(replacement, []byte("new replacement"), 0o644))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(replacement, path))
	newGeneration := fileGeneration(path)
	require.NotEqual(t, oldGeneration, newGeneration)
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), encodeDeletionQueueValue(oldGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), encodeDeletionQueueValue(fileGeneration(other)))
	// This is a newer lifecycle for the same pathname. It lies beyond the
	// distinct-path boundary and must not be retired by the old success state.
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, path), encodeDeletionQueueValue(newGeneration))
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()

	require.FileExists(t, path, "the old generation must not delete the current file")
	require.Equal(t, int64(2), queue.GetQueueDepth(), "the boundary path and newer generation must remain")

	queue.ProcessBatch()
	require.FileExists(t, path, "the newer generation must survive the boundary pass")
	require.Equal(t, int64(1), queue.GetQueueDepth())

	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_RestartAdvancesWatermarkSequence(t *testing.T) {
	dbDir := t.TempDir()
	meta, err := metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)

	oldPath := filepath.Join(t.TempDir(), "existing-watermark-path")
	futureCutoff := time.Now().Add(time.Hour).UnixNano()
	oldKey := keys.MakeDeletionQueueWatermarkKey(futureCutoff, 7)
	wo := grocksdb.NewDefaultWriteOptions()
	value := encodeSuccessWatermark(futureCutoff, futureCutoff, []string{oldPath}, nil)
	require.NoError(t, meta.Handle().Put(wo, oldKey, value))
	wo.Destroy()
	meta.Close()

	meta, err = metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)
	queue := NewQueue(meta, Config{BatchSize: 1})
	require.Equal(t, uint64(8), queue.watermarkSequence)

	filesDir := t.TempDir()
	first := filepath.Join(filesDir, "first.bin")
	second := filepath.Join(filesDir, "second.bin")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0o644))
	require.NoError(t, os.WriteFile(second, []byte("second"), 0o644))
	require.NoError(t, queue.Add(first))
	require.NoError(t, queue.Add(second))
	queue.ProcessBatch()

	require.Equal(t, uint64(9), queue.watermarkSequence)
	require.Len(t, queue.successWatermarks, 2)
	require.Equal(t, string(oldKey), string(queue.successWatermarks[0].key))
	require.NotEqual(t, string(oldKey), string(queue.successWatermarks[1].key))
	queue.Stop()
	meta.Close()
}

func TestQueue_PersistedSuccessWatermarkProtectsPathReuse(t *testing.T) {
	dbDir := t.TempDir()
	meta, err := metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)

	config := Config{BatchSize: 1}
	queue := NewQueue(meta, config)
	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "reused-after-success.txt")
	other := filepath.Join(filesDir, "other.txt")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))
	pathGeneration := fileGeneration(path)
	otherGeneration := fileGeneration(other)

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), encodeDeletionQueueValue(pathGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), encodeDeletionQueueValue(otherGeneration))
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, path), encodeDeletionQueueValue(pathGeneration))
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.Equal(t, int64(2), queue.GetQueueDepth())

	// Recreate the pathname before the old duplicate is scanned, then reopen the
	// queue. The persisted success watermark must retire that old duplicate
	// without treating the recreated file as a new deletion request.
	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	queue.Stop()
	meta.Close()

	meta, err = metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)
	queue = NewQueue(meta, config)
	defer func() {
		queue.Stop()
		meta.Close()
	}()

	queue.ProcessBatch()

	require.FileExists(t, path, "an old successful duplicate must not delete a recreated pathname after restart")
	require.NoFileExists(t, other)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_PrunePreservesSuccessWatermark(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1
	queue.config.PruneAge = 10 * time.Second

	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "pruned-success.bin")
	other := filepath.Join(filesDir, "pruned-success-other.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))
	pathGeneration := fileGeneration(path)
	otherGeneration := fileGeneration(other)

	now := time.Now()
	oldTimestamp := now.Add(-30 * time.Second).UnixNano()
	prunableDuplicate := now.Add(-20 * time.Second).UnixNano()
	recentDuplicate := now.Add(-5 * time.Second).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(oldTimestamp, path), encodeDeletionQueueValue(pathGeneration))
	batch.Put(keys.MakeDeletionQueueKey(oldTimestamp+1, other), encodeDeletionQueueValue(otherGeneration))
	batch.Put(keys.MakeDeletionQueueKey(prunableDuplicate, path), encodeDeletionQueueValue(pathGeneration))
	batch.Put(keys.MakeDeletionQueueKey(recentDuplicate, path), encodeDeletionQueueValue(pathGeneration))
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	// The first path is deleted at the batch boundary. The two later path
	// entries need a success watermark because they were not scanned yet.
	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.NotEmpty(t, queue.successWatermarks)

	// Prune only the older remaining duplicate while the newer one remains in
	// the queue. The watermark must survive this partial cleanup.
	queue.pruneOldEntries()
	state, ok := queue.retryStates[path]
	require.True(t, ok)
	require.NotEmpty(t, state.watermarkKey)
	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	queue.ProcessBatch()

	require.FileExists(t, path, "pruning an old duplicate must not remove success protection from a newer duplicate")
	require.Equal(t, int64(2), queue.processed, "the recreated pathname must not be deleted by an old duplicate")
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_PersistedRetryWatermark(t *testing.T) {
	dbDir := t.TempDir()
	meta, err := metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)

	config := Config{
		BatchSize:  1,
		RetryDelay: time.Hour,
	}
	queue := NewQueue(meta, config)

	filesDir := t.TempDir()
	stuck := filepath.Join(filesDir, "persisted-stuck.txt")
	tail := filepath.Join(filesDir, "persisted-tail.txt")
	require.NoError(t, os.WriteFile(stuck, []byte("stuck"), 0o644))
	require.NoError(t, os.WriteFile(tail, []byte("tail"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, stuck), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, tail), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, stuck), []byte{0x01})
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	lock := fd.GetFileLockManager().GetFileLock(stuck)
	lock.RLock()
	queue.ProcessBatch()
	lock.RUnlock()
	queue.Stop()
	meta.Close()

	meta, err = metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)
	queue = NewQueue(meta, config)
	defer func() {
		queue.Stop()
		meta.Close()
	}()

	queue.ProcessBatch()

	require.FileExists(t, stuck)
	require.NoFileExists(t, tail)
	require.Equal(t, int64(0), queue.failed, "a persisted watermark must suppress the old duplicate after restart")
	require.Equal(t, int64(1), queue.GetQueueDepth(), "only the delayed retry should remain")
}

func TestQueue_PruneOldDuplicateRemovesDelayedRetry(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1
	queue.config.RetryDelay = time.Hour
	queue.config.PruneAge = time.Nanosecond

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "pruned-retry.bin")
	other := filepath.Join(tmpDir, "pruned-other.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+2, path), []byte{0x01})
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	lock := fd.GetFileLockManager().GetFileLock(path)
	lock.RLock()
	queue.ProcessBatch()
	lock.RUnlock()
	require.Equal(t, int64(3), queue.GetQueueDepth(), "the delayed retry, other path, and old duplicate should remain")

	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Remove(other))
	queue.pruneOldEntries()
	require.Equal(t, int64(0), queue.GetQueueDepth(), "pruning a missing old duplicate must remove its delayed retry")

	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()
	require.NoFileExists(t, path)
	require.Equal(t, int64(1), queue.processed, "the recreated pathname should have one new lifecycle")
}

func TestQueue_SuccessfulRetryRemovesFailedMarker(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.BatchSize = 1

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "successful-retry-marker.bin")
	other := filepath.Join(tmpDir, "successful-retry-tail.bin")
	require.NoError(t, os.WriteFile(path, []byte("path"), 0o644))
	require.NoError(t, os.WriteFile(other, []byte("other"), 0o644))

	baseTimestamp := time.Now().Add(-time.Minute).UnixNano()
	oldState := retryState{cutoff: baseTimestamp - 2, retryAt: baseTimestamp - 1}
	queue.retryStates[path] = oldState
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp, path), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueKey(baseTimestamp+1, other), []byte{0x01})
	batch.Put(keys.MakeDeletionQueueRetryStateKey(path), encodeRetryState(oldState))
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, queue.meta.Handle().Write(wo, batch))

	queue.ProcessBatch()

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := queue.meta.Handle().NewIterator(ro)
	defer it.Close()
	prefix := []byte(keys.DeletionQueueRetryStatePrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		require.NotEqual(t, keys.MakeDeletionQueueRetryStateKey(path), key.Data(), "a successful retry must not leave the failed marker behind")
		key.Free()
		it.Value().Free()
	}
}

func TestQueue_PathReuseSupersedesDelayedRetry(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.RetryDelay = time.Hour

	path := filepath.Join(t.TempDir(), "reused-delayed.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	lock := fd.GetFileLockManager().GetFileLock(path)
	lock.RLock()
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed)
	lock.RUnlock()

	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()

	require.NoFileExists(t, path)
	require.Equal(t, int64(1), queue.failed)
	require.Equal(t, int64(1), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth(), "a new lifecycle must remove the superseded delayed retry")
}

func TestQueue_PathReuseSupersedesDueDelayedRetry(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()
	queue.config.RetryDelay = 20 * time.Millisecond

	path := filepath.Join(t.TempDir(), "reused-due-retry.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	lock := fd.GetFileLockManager().GetFileLock(path)
	lock.RLock()
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed)
	retryState, ok := queue.retryStates[path]
	require.True(t, ok)
	lock.RUnlock()

	// Let the old retry become due before the pathname is reused. Add must
	// replace that retry with a protection cutoff, or the stale row is scanned
	// first and deletes the new file before its own queue entry is reached.
	time.Sleep(time.Until(time.Unix(0, retryState.retryAt)) + time.Millisecond)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	require.NoError(t, queue.Add(path))

	queue.ProcessBatch()

	require.NoFileExists(t, path)
	require.Equal(t, int64(1), queue.failed, "the superseded retry must not make another attempt")
	require.Equal(t, int64(1), queue.processed, "the new lifecycle must be the only successful attempt")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "the due retry must be removed with the new lifecycle")
}

func TestQueue_PathReuseSupersedesDueDelayedRetryAfterRestart(t *testing.T) {
	dbDir := t.TempDir()
	meta, err := metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)

	config := Config{BatchSize: 1, RetryDelay: 20 * time.Millisecond}
	queue := NewQueue(meta, config)
	filesDir := t.TempDir()
	path := filepath.Join(filesDir, "reused-due-retry-restart.bin")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	lock := fd.GetFileLockManager().GetFileLock(path)
	lock.RLock()
	require.NoError(t, queue.Add(path))
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed)
	retryState, ok := queue.retryStates[path]
	require.True(t, ok)
	lock.RUnlock()

	time.Sleep(time.Until(time.Unix(0, retryState.retryAt)) + time.Millisecond)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("new"), 0o644))
	require.NoError(t, queue.Add(path))

	queue.Stop()
	meta.Close()
	meta, err = metadata.NewMetaDB(dbDir, 0, nil, nil)
	require.NoError(t, err)
	queue = NewQueue(meta, config)
	defer func() {
		queue.Stop()
		meta.Close()
	}()

	queue.ProcessBatch()

	require.NoFileExists(t, path)
	require.Equal(t, int64(1), queue.processed, "the new lifecycle must be the only successful attempt after restart")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "the stale due retry must not survive restart")
}

func TestQueue_EmptyFilepath(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Try to add empty filepath
	err := queue.Add("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty filepath")
}

func TestQueue_NonExistentFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Add non-existent file
	err := queue.Add("/non/existent/file.txt")
	require.NoError(t, err)

	// Process batch - should handle gracefully
	queue.ProcessBatch()

	// Should count as processed (already deleted)
	require.Equal(t, int64(1), queue.processed)
}

func TestQueue_ConcurrentAdd(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	tmpDir := t.TempDir()
	numFiles := 100
	var wg sync.WaitGroup

	// Create files concurrently
	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			file := filepath.Join(tmpDir, fmt.Sprintf("concurrent_%d.txt", idx))
			err := os.WriteFile(file, []byte("test"), 0o644)
			require.NoError(t, err)
			err = queue.Add(file)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Process in batches
	for i := 0; i < (numFiles/queue.config.BatchSize)+1; i++ {
		queue.ProcessBatch()
	}

	// Verify all files are deleted
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries, "all files should be deleted")
}

func TestQueue_ConcurrentDuplicateAdd(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	testFile := filepath.Join(t.TempDir(), "concurrent-duplicate.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o644))

	const duplicateCount = 100
	var wg sync.WaitGroup
	errs := make(chan error, duplicateCount)
	for i := 0; i < duplicateCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- queue.Add(testFile)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	queue.ProcessBatch()

	require.NoFileExists(t, testFile)
	require.Equal(t, int64(1), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_BackgroundProcessing(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Start background processing
	queue.Start()

	// Create and add test files
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "background.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	err = queue.Add(testFile)
	require.NoError(t, err)

	// Wait for background processing
	time.Sleep(200 * time.Millisecond)

	// Verify file is deleted
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted by background processing")
}

func TestQueue_LockedFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create a test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "locked.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Lock the file using the file lock manager
	lockManager := fd.GetFileLockManager()
	lock := lockManager.GetFileLock(testFile)
	lock.Lock()

	// Add to queue
	err = queue.Add(testFile)
	require.NoError(t, err)

	// Process batch - should fail to delete
	queue.ProcessBatch()

	// File should still exist (couldn't delete due to being locked)
	_, err = os.Stat(testFile)
	require.NoError(t, err, "file should still exist")

	// Should count as failed
	require.Equal(t, int64(1), queue.failed)

	// Unlock the file
	lock.Unlock()

	// Process again - should succeed now
	queue.ProcessBatch()

	// File should be deleted now
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted after lock released")
}

func TestQueue_PruneOldEntries(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Set very short prune age for testing
	queue.config.PruneAge = 100 * time.Millisecond

	// Add a non-existent file (so it won't be deleted)
	err := queue.Add("/old/entry/that/wont/delete.txt")
	require.NoError(t, err)

	// Wait for entry to become old
	time.Sleep(150 * time.Millisecond)

	// Run pruning
	queue.pruneOldEntries()

	// Check that entry was pruned
	require.Equal(t, int64(1), queue.pruned)

	// Verify queue is empty
	depth := queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)
}

// TestQueue_PruneOldEntries_KeepsExistingFile is the regression test for the
// secondary leak in issue #156: pruneOldEntries used to drop queue entries by
// age alone, abandoning a file that was still on disk (e.g. a deletion that kept
// failing because the file was read-locked). The queue entry is the only durable
// record that the file must be deleted, so dropping it orphans the file
// permanently. Prune must keep an aged entry whose file still exists; only the
// normal retry path (ProcessBatch) may reclaim it once deletion succeeds.
func TestQueue_PruneOldEntries_KeepsExistingFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Set very short prune age so the entry ages out quickly.
	queue.config.PruneAge = 100 * time.Millisecond

	// A real file on disk that the queue is responsible for reclaiming.
	tmpDir := t.TempDir()
	existing := filepath.Join(tmpDir, "still-here.bin")
	require.NoError(t, os.WriteFile(existing, []byte("data"), 0o644))

	require.NoError(t, queue.Add(existing))

	// Let the entry age past PruneAge.
	time.Sleep(150 * time.Millisecond)

	queue.pruneOldEntries()

	// The file still exists, so the entry MUST NOT be pruned — dropping it would
	// orphan the file permanently.
	require.Equal(t, int64(0), queue.pruned, "entry whose file still exists must not be pruned")
	require.Equal(t, int64(1), queue.GetQueueDepth(), "aged entry must be kept for retry while its file exists")
	require.FileExists(t, existing)

	// The normal retry path still reclaims the file (and removes the entry) once
	// the deletion can succeed.
	queue.ProcessBatch()
	require.NoFileExists(t, existing, "ProcessBatch should delete the file once it is no longer locked")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "queue should drain after successful deletion")
}

// TestQueue_ProcessBatch_StuckEntriesRequeuedDoNotStarve verifies that
// undeletable entries (read-locked, so their deletion keeps failing) are
// re-enqueued to the tail rather than left blocking the head, so newer deletable
// entries are still reclaimed even when the stuck set EXCEEDS BatchSize. Without
// re-enqueue the oldest BatchSize stuck entries would fill every scan and the
// deletable file behind them would never be reached.
func TestQueue_ProcessBatch_StuckEntriesRequeuedDoNotStarve(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// BatchSize smaller than the stuck set: the stuck head alone would fill a
	// whole batch, so reaching the deletable file proves the head advances.
	queue.config.BatchSize = 2

	tmp := t.TempDir()

	// Three read-locked files at the head whose deletion can't succeed.
	stuck := []string{
		filepath.Join(tmp, "a-stuck"),
		filepath.Join(tmp, "b-stuck"),
		filepath.Join(tmp, "c-stuck"),
	}
	for _, f := range stuck {
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
		require.NoError(t, queue.Add(f))
		lock := fd.GetFileLockManager().GetFileLock(f)
		lock.RLock()
		defer lock.RUnlock()
	}

	// A newer, deletable file enqueued behind the (oversized) stuck head.
	free := filepath.Join(tmp, "d-free")
	require.NoError(t, os.WriteFile(free, []byte("x"), 0o644))
	require.NoError(t, queue.Add(free))

	// A few cycles: failed entries rotate to the tail until the deletable file
	// reaches the head.
	for i := 0; i < 5; i++ {
		queue.ProcessBatch()
	}

	require.NoFileExists(t, free, "deletable file must be reclaimed even when the stuck set exceeds BatchSize")
	for _, f := range stuck {
		require.FileExists(t, f, "read-locked file must not be deleted")
	}
	// The stuck files are still tracked (re-enqueued), never dropped.
	require.Equal(t, int64(len(stuck)), queue.GetQueueDepth(), "stuck files remain queued for retry")
}

// TestQueue_ProcessBatch_RequeuedStuckFileReclaimedAfterUnlock confirms a stuck,
// re-enqueued file is reclaimed once it becomes deletable (its reader releases
// the lock), i.e. re-enqueue defers deletion rather than dropping it.
func TestQueue_ProcessBatch_RequeuedStuckFileReclaimedAfterUnlock(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	tmp := t.TempDir()
	f := filepath.Join(tmp, "temporarily-locked.bin")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	require.NoError(t, queue.Add(f))

	lock := fd.GetFileLockManager().GetFileLock(f)
	lock.RLock()

	// Deletion fails while locked; the entry is re-enqueued, not dropped.
	queue.ProcessBatch()
	require.FileExists(t, f, "locked file must not be deleted")
	require.Equal(t, int64(1), queue.GetQueueDepth(), "entry must stay queued for retry")

	// Once unlocked, the next cycle reclaims it.
	lock.RUnlock()
	queue.ProcessBatch()
	require.NoFileExists(t, f, "file must be reclaimed after the lock is released")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "queue should drain after successful deletion")
}

// TestQueue_ProcessBatch_RetryDelayDefersRetry verifies that a failed deletion
// is re-enqueued under a future timestamp (now+RetryDelay) and is not retried
// until that backoff elapses — bounding retry churn for persistently-stuck
// files rather than rewriting them every cycle.
func TestQueue_ProcessBatch_RetryDelayDefersRetry(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	queue.config.RetryDelay = 200 * time.Millisecond

	tmp := t.TempDir()
	f := filepath.Join(tmp, "locked.bin")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	require.NoError(t, queue.Add(f))

	lock := fd.GetFileLockManager().GetFileLock(f)
	lock.RLock()
	defer lock.RUnlock()

	// First attempt fails (locked) and re-enqueues at now+RetryDelay.
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed)
	require.Equal(t, int64(1), queue.GetQueueDepth(), "entry stays queued for retry")

	// Within the backoff window the entry is not due: a second cycle skips it,
	// so there is no extra failure and no rewrite.
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed, "stuck entry must not be retried before RetryDelay elapses")

	// After the backoff elapses the entry is due again and is retried.
	time.Sleep(250 * time.Millisecond)
	queue.ProcessBatch()
	require.Equal(t, int64(2), queue.failed, "stuck entry must be retried after RetryDelay elapses")
}

func TestQueue_ProcessBatch_DuplicateRetryCoalesces(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	queue.config.RetryDelay = 200 * time.Millisecond

	f := filepath.Join(t.TempDir(), "duplicate-locked.bin")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	lock := fd.GetFileLockManager().GetFileLock(f)
	lock.RLock()

	const duplicateCount = 5
	for i := 0; i < duplicateCount; i++ {
		require.NoError(t, queue.Add(f))
		time.Sleep(time.Millisecond)
	}

	// One failed filesystem attempt replaces every due duplicate with one
	// delayed retry.
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed)
	require.Equal(t, int64(1), queue.GetQueueDepth(), "duplicate failures must share one retry")
	require.FileExists(t, f)

	// The coalesced retry is not attempted again during its backoff window.
	queue.ProcessBatch()
	require.Equal(t, int64(1), queue.failed, "coalesced retry must respect RetryDelay")
	require.Equal(t, int64(1), queue.GetQueueDepth())

	lock.RUnlock()
	time.Sleep(250 * time.Millisecond)
	queue.ProcessBatch()

	require.NoFileExists(t, f)
	require.Equal(t, int64(1), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_PathReuseStartsNewDeletionLifecycle(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	f := filepath.Join(t.TempDir(), "reused.bin")
	require.NoError(t, os.WriteFile(f, []byte("first"), 0o644))
	require.NoError(t, queue.Add(f))
	queue.ProcessBatch()
	require.NoFileExists(t, f)
	require.Equal(t, int64(0), queue.GetQueueDepth())

	// A later file at the same path gets its own queue key and is processed
	// independently after the first lifecycle has drained.
	require.NoError(t, os.WriteFile(f, []byte("second"), 0o644))
	require.NoError(t, queue.Add(f))
	require.Equal(t, int64(1), queue.GetQueueDepth())
	queue.ProcessBatch()

	require.NoFileExists(t, f)
	require.Equal(t, int64(2), queue.processed)
	require.Equal(t, int64(0), queue.GetQueueDepth())
}

func TestQueue_GetQueueDepth(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Initially empty
	depth := queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)

	// Add some entries
	for i := 0; i < 5; i++ {
		err := queue.Add(fmt.Sprintf("/test/file%d.txt", i))
		require.NoError(t, err)
	}

	// Check depth
	depth = queue.GetQueueDepth()
	require.Equal(t, int64(5), depth)

	// Process batch
	queue.ProcessBatch()

	// Should be empty after processing
	depth = queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)
}

func TestQueue_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	meta, err := metadata.NewMetaDB(tmpDir, 0, nil, nil) // nil merge operator for tests
	require.NoError(t, err)
	defer meta.Close()

	config := Config{
		BatchSize:       10,
		ProcessInterval: 50 * time.Millisecond,
		PruneAge:        1 * time.Hour,
	}

	queue := NewQueue(meta, config)
	queue.Start()

	// Add some files
	for i := 0; i < 3; i++ {
		err := queue.Add(fmt.Sprintf("/test/ctx_%d.txt", i))
		require.NoError(t, err)
	}

	// Stop the queue (cancels context)
	queue.Stop()

	// Verify the background loop stopped
	// The WaitGroup should have completed
	done := make(chan struct{})
	go func() {
		queue.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good, background loop stopped
	case <-time.After(1 * time.Second):
		t.Fatal("background loop did not stop after context cancellation")
	}
}

func TestQueue_RetryStateEncoding(t *testing.T) {
	state := retryState{
		cutoff:          101,
		duplicateCutoff: 202,
		retryAt:         303,
		generation:      "file:dev=1,ino=2",
	}

	decoded, ok := decodeRetryState(encodeRetryState(state))
	require.True(t, ok)
	require.Equal(t, state, decoded)

	legacy, ok := decodeRetryState(append(
		uint64Bytes(uint64(state.cutoff)),
		uint64Bytes(uint64(state.retryAt))...,
	))
	require.True(t, ok)
	require.Equal(t, state.cutoff, legacy.cutoff)
	require.Equal(t, state.cutoff, legacy.duplicateCutoff)
	require.Equal(t, state.retryAt, legacy.retryAt)
	require.Empty(t, legacy.generation)
}

func uint64Bytes(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func TestQueue_KeyFunctions(t *testing.T) {
	// Test MakeDeletionQueueKey
	key := keys.MakeDeletionQueueKey(1234567890123456789, "/path/to/file.txt")
	expected := []byte("!del/01234567890123456789//path/to/file.txt")
	require.Equal(t, expected, key)

	// Test ParseDeletionQueueKey
	timestamp, filepath, err := keys.ParseDeletionQueueKey(key)
	require.NoError(t, err)
	require.Equal(t, int64(1234567890123456789), timestamp)
	require.Equal(t, "/path/to/file.txt", filepath)

	// Test with malformed key
	badKey := []byte("malformed")
	_, _, err = keys.ParseDeletionQueueKey(badKey)
	require.Error(t, err)

	// Test IsDeletionQueueKey
	require.True(t, keys.IsDeletionQueueKey(key))
	require.False(t, keys.IsDeletionQueueKey(badKey))
}
