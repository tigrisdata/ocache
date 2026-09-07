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

	s.cleaner.reconcileFromMetadata()

	assert.Eventually(t, func() bool { return !fileExists(aged) }, 10*time.Second, 50*time.Millisecond,
		"an unreferenced raw file older than the grace window must be reclaimed")
	assert.True(t, fileExists(recent), "a file inside the grace window may be an in-flight put and must be kept")
	assert.Equal(t, string(live), readAllValue(t, s, "live"))
	assert.Equal(t, float64(2048+4096+4096), testutil.ToFloat64(metrics.FilesDirBytes),
		"physical files/ size is published as measured before reclaim")

	// Once the recent orphan ages past the window, the next pass takes it.
	old := time.Now().Add(-2 * orphanGraceWindow)
	require.NoError(t, os.Chtimes(recent, old, old))
	s.cleaner.reconcileFromMetadata()
	assert.Eventually(t, func() bool { return !fileExists(recent) }, 10*time.Second, 50*time.Millisecond)
	assert.Equal(t, string(live), readAllValue(t, s, "live"))
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
