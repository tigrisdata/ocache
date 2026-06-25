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

	// The read must self-heal: a clean miss, not an error.
	r2, found2, err2 := stor.Get(key, 0, 0)
	assert.NoError(t, err2, "dangling large raw file must not surface as an error")
	assert.False(t, found2)
	assert.Nil(t, r2)

	// The dangling key must be tombstoned so subsequent reads also miss.
	r3, found3, err3 := stor.Get(key, 0, 0)
	assert.NoError(t, err3)
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

	// A stale reader holding the p1 snapshot would call purge with the old path.
	purged := stor.purgeDanglingRawFile(key, p1, int64(len(v1)))
	assert.False(t, purged, "must not purge when metadata now references a different file")

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
