// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// danglingTestStorage builds a Storage with tiny thresholds so that a small
// payload still lands as a raw file: > inlineThreshold makes it a RAW_FILE, and
// > compactThreshold makes it "large" (never compacted, not in the compaction
// index) — the exact class that can crash-loop on a dangling reference (#150).
func danglingTestStorage(t *testing.T, tmpDir string) *Storage {
	config := &StorageConfig{
		DiskPath:         tmpDir,
		MaxDiskUsage:     100 * 1024 * 1024,
		InlineThreshold:  1024,     // > 1KB → raw file
		CompactThreshold: 4 * 1024, // > 4KB → large (never compacted)
		SegmentSize:      256 * 1024 * 1024,
		CleanupInterval:  0, // Disable cleanup so the sentinel isn't swept mid-test
	}
	stor, err := NewStorageWithConfig(config)
	require.NoError(t, err)
	return stor
}

// rawFilePathOf reads the stored metadata for key and returns its RawFilePath.
func rawFilePathOf(t *testing.T, stor *Storage, key string) (string, int64) {
	t.Helper()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := stor.meta.Handle().Get(ro, keys.MakeMetadataKey(key))
	require.NoError(t, err)
	defer slice.Free()
	require.True(t, slice.Exists())
	vm := &pb.ValueMessage{}
	require.NoError(t, proto.Unmarshal(slice.Data(), vm))
	return vm.RawFilePath, vm.ValueLength
}

// TestStorage_Get_DanglingLargeRawFile_SelfHeals verifies that a large raw-file
// reference whose backing file has vanished (e.g. a write lost to an unclean
// shutdown) is treated as a cache miss and purged, rather than erroring/crash-
// looping on every read.
func TestStorage_Get_DanglingLargeRawFile_SelfHeals(t *testing.T) {
	tmpDir := t.TempDir()
	stor := danglingTestStorage(t, tmpDir)

	key := "dangling-large"
	value := bytes.Repeat([]byte("x"), 8*1024) // 8KB > compactThreshold → large
	require.NoError(t, stor.Put(key, bytes.NewReader(value), 0))

	// Confirm it reads back before we sabotage it.
	r, found, err := stor.Get(key, 0, 0)
	require.NoError(t, err)
	require.True(t, found)
	got, _ := io.ReadAll(r)
	require.Equal(t, value, got)
	if rc, ok := r.(io.Closer); ok {
		rc.Close()
	}

	// Simulate the dangling reference: remove the backing file and evict any
	// cached descriptor so the next read re-opens and observes ENOENT.
	rawPath, _ := rawFilePathOf(t, stor, key)
	require.NoError(t, os.Remove(rawPath))
	fd.GetFdCache().Remove(rawPath)

	// First read observes ENOENT, issues the purge, and returns a retryable
	// error so the internal Get retry (in GetLocal) re-drives and reads the
	// tombstone. (storage.Get is called directly here, bypassing that retry.)
	r2, found2, err2 := stor.Get(key, 0, 0)
	assert.Error(t, err2, "first dangling read returns a retryable error to drive the retry")
	assert.True(t, storageErrors.IsRetryable(err2))
	assert.False(t, found2)
	assert.Nil(t, r2)

	// The purge tombstone is now visible, so subsequent reads are clean misses.
	r3, found3, err3 := stor.Get(key, 0, 0)
	assert.NoError(t, err3, "after purge the dangling key reads as a clean miss")
	assert.False(t, found3)
	assert.Nil(t, r3)
}

// TestStorage_Get_DanglingMediumRawFile_StaysRetryable verifies that a missing
// medium raw file is NOT purged: medium files are migrated to segments by the
// compactor, which briefly unlinks the raw file before its metadata CAS lands,
// so ENOENT there is transient and must stay retryable.
func TestStorage_Get_DanglingMediumRawFile_StaysRetryable(t *testing.T) {
	tmpDir := t.TempDir()
	stor := danglingTestStorage(t, tmpDir)

	key := "dangling-medium"
	value := bytes.Repeat([]byte("y"), 2*1024) // 1KB < 2KB <= 4KB → medium
	require.NoError(t, stor.Put(key, bytes.NewReader(value), 0))

	rawPath, _ := rawFilePathOf(t, stor, key)
	require.NoError(t, os.Remove(rawPath))
	fd.GetFdCache().Remove(rawPath)

	_, found, err := stor.Get(key, 0, 0)
	assert.Error(t, err, "missing medium raw file must surface as a retryable error")
	assert.True(t, storageErrors.IsRetryable(err))
	assert.False(t, found)

	// Metadata must be left intact (still a RAW_FILE pointing at the same path).
	gotPath, _ := rawFilePathOf(t, stor, key)
	assert.Equal(t, rawPath, gotPath, "medium dangling key must not be purged")
}

// TestStorage_PurgeDanglingRawFile_ConcurrentOverwriteNotPurged covers the race
// where a Put replaces the key (with a fresh file path) between a reader's
// metadata snapshot and its stale ENOENT file read. The purge must observe that
// the metadata no longer references the old path, decline to purge, and report
// not-dangling so the caller retries instead of reporting a spurious miss.
func TestStorage_PurgeDanglingRawFile_ConcurrentOverwriteNotPurged(t *testing.T) {
	tmpDir := t.TempDir()
	stor := danglingTestStorage(t, tmpDir)

	key := "raced-large"
	v1 := bytes.Repeat([]byte("a"), 8*1024) // large
	require.NoError(t, stor.Put(key, bytes.NewReader(v1), 0))
	p1, _ := rawFilePathOf(t, stor, key)

	// A concurrent Put replaces the key with a fresh large value (new path).
	v2 := bytes.Repeat([]byte("b"), 8*1024)
	require.NoError(t, stor.Put(key, bytes.NewReader(v2), 0))
	p2, _ := rawFilePathOf(t, stor, key)
	require.NotEqual(t, p1, p2)

	// A stale reader holding the p1 snapshot calls purge with the OLD path. The
	// CAS precondition (p1) no longer matches the live metadata (p2), so the
	// merge must be a no-op and leave the live value untouched.
	stor.purgeDanglingRawFile(key, p1)

	// The live value (v2/p2) must be intact and still readable.
	gotPath, _ := rawFilePathOf(t, stor, key)
	assert.Equal(t, p2, gotPath, "live value must not be clobbered")
	r, found, err := stor.Get(key, 0, 0)
	require.NoError(t, err)
	require.True(t, found)
	got, _ := io.ReadAll(r)
	assert.Equal(t, v2, got)
	if rc, ok := r.(io.Closer); ok {
		rc.Close()
	}
}

func readCASValueAndClose(t *testing.T, stor *Storage, key string) (uint64, []byte) {
	t.Helper()

	r, version, found, err := stor.GetWithVersion(key)
	require.NoError(t, err)
	require.True(t, found)
	data, readErr := io.ReadAll(r)
	var closeErr error
	if rc, ok := r.(io.Closer); ok {
		closeErr = rc.Close()
	}
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	return version, data
}

func TestCAS_GetWithVersion_DanglingLargeRawFile_Recovery(t *testing.T) {
	t.Run("recovers_with_a_refill_token", func(t *testing.T) {
		stor := danglingTestStorage(t, t.TempDir())
		defer stor.Close()
		// The medium-file control below needs a stable foreground state. Large
		// raw files are not in the compaction index, so stopping the worker is
		// otherwise immaterial to these fault fixtures.
		stor.compactor.Close()

		// A separate plain-read key proves that the existing recovery branch is
		// reachable without mixing plain and CAS operations on one key.
		plainKey := "dangling-plain-comparator"
		plainValue := bytes.Repeat([]byte("p"), 8*1024)
		require.NoError(t, stor.Put(plainKey, bytes.NewReader(plainValue), 0))
		plainPath, _ := rawFilePathOf(t, stor, plainKey)
		require.NoError(t, os.Remove(plainPath))
		fd.GetFdCache().Remove(plainPath)
		_, found, err := stor.Get(plainKey, 0, 0)
		require.Error(t, err)
		require.True(t, storageErrors.IsRetryable(err))
		require.False(t, found)
		_, found, err = stor.Get(plainKey, 0, 0)
		require.NoError(t, err)
		require.False(t, found)

		key := "dangling-large-cas"
		value := bytes.Repeat([]byte("x"), 8*1024) // >4 KiB: never compacted
		_, err = stor.PutIfVersion(key, bytes.NewReader(value), 0, 0)
		require.NoError(t, err)
		rawPath, length := rawFilePathOf(t, stor, key)
		require.Equal(t, int64(len(value)), length)
		require.NoError(t, os.Remove(rawPath))
		fd.GetFdCache().Remove(rawPath)

		// A single failed open is retryable so the idempotent local wrapper can
		// re-read. The next direct open must see the authoritative purge result.
		_, _, found, err = stor.GetWithVersion(key)
		require.Error(t, err)
		require.True(t, storageErrors.IsRetryable(err))
		require.False(t, found)
		_, token, found, err := stor.GetWithVersion(key)
		require.NoError(t, err, "the retry must re-read the purged metadata")
		require.False(t, found)
		require.NotZero(t, token, "absence must carry a refill observation token")

		refill := []byte("refilled with the observed token")
		refillVersion, err := stor.PutIfVersion(key, bytes.NewReader(refill), 0, token)
		require.NoError(t, err)
		gotVersion, got := readCASValueAndClose(t, stor, key)
		assert.Equal(t, refillVersion, gotVersion)
		assert.Equal(t, refill, got)

		// A delete stamped after the observation must keep rejecting that older
		// token, even after the key was refilled once.
		require.NoError(t, stor.DeleteIfVersion(key, refillVersion))
		_, fence, found, err := stor.GetWithVersion(key)
		require.NoError(t, err)
		require.False(t, found)
		require.Greater(t, fence, token)
		_, err = stor.PutIfVersion(key, bytes.NewReader(refill), 0, token)
		mismatch, ok := storageErrors.IsVersionMismatch(err)
		require.True(t, ok, "an intervening CAS delete must reject the earlier absence token")
		assert.Equal(t, fence, mismatch.CurrentVersion)
	})

	t.Run("stale_purges_preserve_replacement_and_delete_fence", func(t *testing.T) {
		stor := danglingTestStorage(t, t.TempDir())
		defer stor.Close()
		stor.compactor.Close()

		key := "dangling-cas-replacement"
		oldValue := bytes.Repeat([]byte("a"), 8*1024)
		oldVersion, err := stor.PutIfVersion(key, bytes.NewReader(oldValue), 0, 0)
		require.NoError(t, err)
		oldPath, _ := rawFilePathOf(t, stor, key)
		newValue := bytes.Repeat([]byte("b"), 8*1024)
		newVersion, err := stor.PutIfVersion(key, bytes.NewReader(newValue), 0, oldVersion)
		require.NoError(t, err)
		newPath, _ := rawFilePathOf(t, stor, key)
		require.NotEqual(t, oldPath, newPath, "CAS replacement must use a fresh raw-file path")

		stor.purgeDanglingRawFile(key, oldPath)
		gotVersion, got := readCASValueAndClose(t, stor, key)
		assert.Equal(t, newVersion, gotVersion)
		assert.Equal(t, newValue, got)

		deleteKey := "dangling-cas-deleted"
		deleteVersion, err := stor.PutIfVersion(deleteKey, bytes.NewReader(oldValue), 0, 0)
		require.NoError(t, err)
		deletePath, _ := rawFilePathOf(t, stor, deleteKey)
		require.NoError(t, stor.DeleteIfVersion(deleteKey, deleteVersion))
		_, fenceBefore, found, err := stor.GetWithVersion(deleteKey)
		require.NoError(t, err)
		require.False(t, found)
		require.NotZero(t, fenceBefore)

		stor.purgeDanglingRawFile(deleteKey, deletePath)
		_, fenceAfter, found, err := stor.GetWithVersion(deleteKey)
		require.NoError(t, err)
		require.False(t, found)
		assert.Equal(t, fenceBefore, fenceAfter, "a stale purge must preserve the CAS delete fence")
		_, err = stor.PutIfVersion(deleteKey, bytes.NewReader(newValue), 0, deleteVersion)
		mismatch, ok := storageErrors.IsVersionMismatch(err)
		require.True(t, ok)
		assert.Equal(t, fenceBefore, mismatch.CurrentVersion)
	})

	t.Run("missing_medium_raw_file_stays_retryable", func(t *testing.T) {
		stor := danglingTestStorage(t, t.TempDir())
		defer stor.Close()
		stor.compactor.Close()

		key := "dangling-medium-cas"
		value := bytes.Repeat([]byte("m"), 2*1024) // 1 KiB < size <= 4 KiB
		_, err := stor.PutIfVersion(key, bytes.NewReader(value), 0, 0)
		require.NoError(t, err)
		rawPath, length := rawFilePathOf(t, stor, key)
		require.Equal(t, int64(len(value)), length)
		require.NoError(t, os.Remove(rawPath))
		fd.GetFdCache().Remove(rawPath)

		_, _, found, err := stor.GetWithVersion(key)
		require.Error(t, err)
		assert.True(t, storageErrors.IsRetryable(err))
		assert.False(t, found)
		gotPath, gotLength := rawFilePathOf(t, stor, key)
		assert.Equal(t, rawPath, gotPath, "a missing medium file must not be purged by the foreground CAS read")
		assert.Equal(t, length, gotLength)
	})
}
