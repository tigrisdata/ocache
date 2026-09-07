// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
)

func newOrphanTestStorage(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         dir,
		InlineThreshold:  1024,
		CompactThreshold: 1024, // every raw file here is a permanent large file: never migrated by the compactor
		SegmentSize:      16 * 1024 * 1024,
		FdCacheSize:      100,
		CleanupInterval:  time.Hour,
	})
	require.NoError(t, err)
	return s
}

// dropOrphan writes an unreferenced file into files/ with the given age.
func dropOrphan(t *testing.T, dir, name string, size int, age time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, "files", name)
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("o"), size), 0o644))
	when := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(path, when, when))
	return path
}

// forceSweep makes the next reconcile run the orphan sweep regardless of when
// the last one ran (the sweep is otherwise daily after the startup pass).
func forceSweep(s *Storage) { s.cleaner.lastOrphanSweep = time.Time{} }

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readAllValue(t *testing.T, s *Storage, key string) string {
	t.Helper()
	r, found, err := s.Get(key, 0, 0)
	require.NoError(t, err)
	require.True(t, found, "live key %q must survive the sweep", key)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	if c, ok := r.(io.Closer); ok {
		require.NoError(t, c.Close())
	}
	return string(data)
}

// TestOrphanSweep_ReclaimsAgedUnreferencedFiles pins the sweep's contract
// (issue #156): a raw file no metadata row references is reclaimed once it is
// older than the grace window, a recent one is left for the next pass, and a
// referenced one is never touched.
func TestOrphanSweep_ReclaimsAgedUnreferencedFiles(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir)
	defer s.Close()

	live := bytes.Repeat([]byte("L"), 2048)
	require.NoError(t, s.Put("live", bytes.NewReader(live), 0))
	aged := dropOrphan(t, dir, "orphan-aged", 4096, time.Hour)
	recent := dropOrphan(t, dir, "orphan-recent", 4096, 0)

	forceSweep(s)
	s.cleaner.reconcileFromMetadata()

	assert.Eventually(t, func() bool { return !fileExists(aged) }, 10*time.Second, 50*time.Millisecond,
		"an unreferenced raw file older than the grace window must be reclaimed")
	assert.True(t, fileExists(recent), "a file inside the grace window may be an in-flight put and must be kept")
	assert.Equal(t, string(live), readAllValue(t, s, "live"))
	assert.Equal(t, float64(2048+4096+4096), testutil.ToFloat64(metrics.FilesDirBytes),
		"files/ size = referenced payload bytes (from metadata) + unreferenced files on disk, as measured before reclaim")

	// Once the recent orphan ages past the window, the next pass takes it.
	old := time.Now().Add(-2 * orphanGraceWindow)
	require.NoError(t, os.Chtimes(recent, old, old))
	forceSweep(s)
	s.cleaner.reconcileFromMetadata()
	assert.Eventually(t, func() bool { return !fileExists(recent) }, 10*time.Second, 50*time.Millisecond)
	assert.Equal(t, string(live), readAllValue(t, s, "live"))
}

// TestOrphanSweep_LeavesActiveWritesAlone pins the two guards that make the
// sweep safe against a write in progress regardless of how old the file's
// mtime is: a file whose lock is held (a client stalled mid-stream, or a
// reader) and a file registered as in-flight (written, not yet committed)
// are both skipped, and each is reclaimed once the guard is released.
func TestOrphanSweep_LeavesActiveWritesAlone(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir)
	defer s.Close()

	stalled := dropOrphan(t, dir, "stalled-upload", 4096, time.Hour)
	lock := fd.GetFileLockManager().GetFileLock(stalled)
	lock.Lock() // FileManager.Write holds this for the whole write and fsync
	uncommitted := dropOrphan(t, dir, "written-not-committed", 4096, time.Hour)
	s.inflightRaw.Store(uncommitted, struct{}{}) // Put holds this until the row commits

	forceSweep(s)
	s.cleaner.reconcileFromMetadata()
	time.Sleep(1500 * time.Millisecond) // longer than the deletion queue's interval
	assert.True(t, fileExists(stalled), "a file whose lock is held is being written and must not be queued")
	assert.True(t, fileExists(uncommitted), "a file registered as in-flight must not be queued")

	lock.Unlock()
	s.inflightRaw.Delete(uncommitted)
	forceSweep(s)
	s.cleaner.reconcileFromMetadata()
	assert.Eventually(t, func() bool { return !fileExists(stalled) && !fileExists(uncommitted) },
		10*time.Second, 50*time.Millisecond, "once the guards are released the aged orphans are reclaimed")
}

// TestPut_RegistersRawFileUntilCommit: Put holds its raw file in the in-flight
// registry from the write through the metadata commit, and releases it after.
func TestPut_RegistersRawFileUntilCommit(t *testing.T) {
	s := newOrphanTestStorage(t, t.TempDir())
	defer s.Close()

	inflightAtCommit := 0
	s.beforeMetaCommit = func() error {
		s.inflightRaw.Range(func(_, _ any) bool { inflightAtCommit++; return true })
		return nil
	}
	require.NoError(t, s.Put("k", bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), 0))
	assert.Equal(t, 1, inflightAtCommit, "the raw file must be registered while its row is being committed")
	after := 0
	s.inflightRaw.Range(func(_, _ any) bool { after++; return true })
	assert.Zero(t, after, "the registration must be released once the row is committed")
}

// TestOrphanSweep_IsDailyAfterStartup: a reconcile inside the interval does
// not sweep (the reference set is not even collected), one past it does.
func TestOrphanSweep_IsDailyAfterStartup(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir) // the startup reconcile has just swept
	defer s.Close()
	require.False(t, s.cleaner.lastOrphanSweep.IsZero(), "startup must run the sweep")

	aged := dropOrphan(t, dir, "orphan-aged", 4096, time.Hour)
	s.cleaner.reconcileFromMetadata() // within the interval: hourly reconcile, no sweep
	time.Sleep(1500 * time.Millisecond)
	assert.True(t, fileExists(aged), "a reconcile inside the sweep interval must not sweep")

	s.cleaner.lastOrphanSweep = time.Now().Add(-orphanSweepInterval)
	s.cleaner.reconcileFromMetadata()
	assert.Eventually(t, func() bool { return !fileExists(aged) }, 10*time.Second, 50*time.Millisecond,
		"a reconcile past the sweep interval must sweep")
}

// TestOrphanSweep_FailedSweepIsRetriedNextReconcile: a sweep that cannot
// read files/ does not advance the daily clock, so the next reconcile tries
// again rather than waiting a day.
func TestOrphanSweep_FailedSweepIsRetriedNextReconcile(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir)
	defer s.Close()

	filesDir := filepath.Join(dir, "files")
	hidden := filepath.Join(dir, "files.hidden")
	require.NoError(t, os.Rename(filesDir, hidden))
	due := time.Now().Add(-orphanSweepInterval)
	s.cleaner.lastOrphanSweep = due
	s.cleaner.reconcileFromMetadata()
	assert.Equal(t, due, s.cleaner.lastOrphanSweep, "a sweep that could not read files/ must not count as done")

	require.NoError(t, os.Rename(hidden, filesDir))
	s.cleaner.reconcileFromMetadata()
	assert.True(t, s.cleaner.lastOrphanSweep.After(due), "the next reconcile must retry and complete the sweep")
}

// TestOrphanSweep_GaugeExcludesDanglingReferences: a row whose raw file is
// missing (purged by the read path on its next miss) contributes nothing to
// the directory-size gauge, because the size of a referenced file is counted
// only when its name is present in the listing.
func TestOrphanSweep_GaugeExcludesDanglingReferences(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir)
	defer s.Close()

	require.NoError(t, s.Put("dangling", bytes.NewReader(bytes.Repeat([]byte("d"), 2048)), 0))
	require.NoError(t, s.Put("present", bytes.NewReader(bytes.Repeat([]byte("p"), 3072)), 0))
	// Remove the first key's file behind the row's back, using the path the row records.
	row, found, err := s.readRowForCAS(keys.MakeMetadataKey("dangling"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, pb.ValueType_RAW_FILE, row.ValueType)
	require.NoError(t, os.Remove(row.RawFilePath))

	forceSweep(s)
	s.cleaner.reconcileFromMetadata()
	assert.Equal(t, float64(3072), testutil.ToFloat64(metrics.FilesDirBytes),
		"only the referenced file that exists on disk counts toward the gauge")
}

// TestOrphanSweep_RunsAtStartup: files leaked by an earlier process (the
// backlog #156 was opened for) are reclaimed by the first reconcile after the
// server starts, without touching data a row still references.
func TestOrphanSweep_RunsAtStartup(t *testing.T) {
	dir := t.TempDir()
	s := newOrphanTestStorage(t, dir)
	live := bytes.Repeat([]byte("L"), 2048)
	require.NoError(t, s.Put("live", bytes.NewReader(live), 0))
	s.Close()

	leaked := dropOrphan(t, dir, "leaked-by-old-process", 8192, 24*time.Hour)

	s = newOrphanTestStorage(t, dir)
	defer s.Close()
	assert.Eventually(t, func() bool { return !fileExists(leaked) }, 10*time.Second, 50*time.Millisecond,
		"a pre-existing orphan must be reclaimed by the startup sweep")
	assert.Equal(t, string(live), readAllValue(t, s, "live"))
}
