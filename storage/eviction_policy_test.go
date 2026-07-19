// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

// countPrefix returns the number of RocksDB keys under the given prefix.
func countPrefix(t *testing.T, s *Storage, prefix []byte) int {
	t.Helper()
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()
	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		n++
		it.Key().Free()
		it.Value().Free()
	}
	return n
}

func countFifoEntries(t *testing.T, s *Storage) int {
	return countPrefix(t, s, keys.GetFifoIndexPrefix())
}

func countAccessBucketEntries(t *testing.T, s *Storage) int {
	return countPrefix(t, s, GetOldestAccessBucketPrefix())
}

// readAccessIndex returns the current bucketed access key recorded for a key via
// the LRU secondary index, or ("", false) if none exists.
func readAccessIndex(t *testing.T, s *Storage, key string) (string, bool) {
	t.Helper()
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := s.meta.Handle().Get(ro, keys.MakeBucketedAccessIndexKey(key))
	require.NoError(t, err)
	defer slice.Free()
	if !slice.Exists() {
		return "", false
	}
	return string(slice.Data()), true
}

// TestEvictionPolicyNormalization checks that the policy is defaulted/validated
// and that the access updater (whose only job is to refresh recency on reads for
// LRU) is created for LRU-with-cap but not for FIFO.
func TestEvictionPolicyNormalization(t *testing.T) {
	cases := []struct {
		name         string
		policy       string
		maxDiskUsage int64
		wantPolicy   string
		wantUpdater  bool
	}{
		{"empty defaults to lru", "", 1 << 20, EvictionPolicyLRU, true},
		{"explicit lru", EvictionPolicyLRU, 1 << 20, EvictionPolicyLRU, true},
		{"fifo has no updater", EvictionPolicyFIFO, 1 << 20, EvictionPolicyFIFO, false},
		{"unknown falls back to lru", "bogus", 1 << 20, EvictionPolicyLRU, true},
		{"no disk cap has no updater", EvictionPolicyLRU, 0, EvictionPolicyLRU, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewStorageWithConfig(&StorageConfig{
				DiskPath:        t.TempDir(),
				InlineThreshold: 1 << 20,
				MaxDiskUsage:    tc.maxDiskUsage,
				EvictionPolicy:  tc.policy,
				CleanupInterval: time.Hour,
			})
			require.NoError(t, err)
			defer s.Close()

			assert.Equal(t, tc.wantPolicy, s.evictionPolicy)
			assert.Equal(t, tc.wantUpdater, s.accessUpdater != nil)
		})
	}
}

// TestFIFOIndexOnWriteNotOnRead verifies FIFO writes a single !fifo/ index entry
// per Put, writes no LRU access-bucket entry, and does not touch the index on
// reads (so a read cannot refresh an entry's write-time position).
func TestFIFOIndexOnWriteNotOnRead(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v")), 0))
	require.Equal(t, 1, countFifoEntries(t, s), "FIFO Put should write one index entry")
	require.Equal(t, 0, countAccessBucketEntries(t, s), "FIFO must not use the LRU access index")

	before := fifoEntryKey(t, s)
	for i := 0; i < 3; i++ {
		_, found, err := s.Get("k", 0, 0)
		require.NoError(t, err)
		require.True(t, found)
	}
	assert.Equal(t, 1, countFifoEntries(t, s), "reads must not add index entries")
	assert.Equal(t, before, fifoEntryKey(t, s), "reads must not move the write-time entry")
}

// fifoEntryKey returns the (single) FIFO index key currently stored.
func fifoEntryKey(t *testing.T, s *Storage) string {
	t.Helper()
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()
	prefix := keys.GetFifoIndexPrefix()
	it.Seek(prefix)
	require.True(t, it.ValidForPrefix(prefix))
	return string(it.Key().Data())
}

// TestLRUReadRebucketsEntry is the LRU contrast: a read re-buckets the entry
// (refreshing recency), which is exactly what FIFO avoids.
func TestLRUReadRebucketsEntry(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:          t.TempDir(),
		InlineThreshold:   1 << 20,
		MaxDiskUsage:      1 << 20,
		EvictionPolicy:    EvictionPolicyLRU,
		AccessUpdateDelay: time.Millisecond,
		CleanupInterval:   time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v")), 0))
	before, ok := readAccessIndex(t, s, "k")
	require.True(t, ok)

	_, _, err = s.Get("k", 0, 0)
	require.NoError(t, err)
	s.FlushAccessUpdates()

	after, ok := readAccessIndex(t, s, "k")
	require.True(t, ok)
	assert.NotEqual(t, before, after, "LRU read should re-bucket the entry")
	assert.Equal(t, 0, countFifoEntries(t, s), "LRU must not use the FIFO index")
}

// readFifoBackref returns the FIFO entry the secondary index points to for a
// key, or ("", false) if none exists.
func readFifoBackref(t *testing.T, s *Storage, key string) (string, bool) {
	t.Helper()
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := s.meta.Handle().Get(ro, keys.MakeFifoBackrefKey(key))
	require.NoError(t, err)
	defer slice.Free()
	if !slice.Exists() {
		return "", false
	}
	return string(slice.Data()), true
}

// TestFIFOOverwriteReplacesEntry verifies that overwriting a key deletes its
// previous FIFO entry (via the back-reference), leaving exactly one entry at the
// new write time — so a rewritten key is ordered by its latest write, not its
// first.
func TestFIFOOverwriteReplacesEntry(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v0")), 0))
	require.Equal(t, 1, countFifoEntries(t, s))
	e1 := fifoEntryKey(t, s)
	ref1, ok := readFifoBackref(t, s, "k")
	require.True(t, ok)
	assert.Equal(t, e1, ref1, "back-reference should point to the current entry")

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v1")), 0))
	assert.Equal(t, 1, countFifoEntries(t, s), "overwrite must not leave a stale entry")
	e2 := fifoEntryKey(t, s)
	assert.NotEqual(t, e1, e2, "overwrite should re-index at the new write time")
	ref2, ok := readFifoBackref(t, s, "k")
	require.True(t, ok)
	assert.Equal(t, e2, ref2)
}

// TestFIFODeleteRemovesEntry verifies that deleting a key removes its FIFO entry
// and back-reference, so removals don't leak index entries.
func TestFIFODeleteRemovesEntry(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v")), 0))
	require.Equal(t, 1, countFifoEntries(t, s))
	_, ok := readFifoBackref(t, s, "k")
	require.True(t, ok)

	require.NoError(t, s.DeleteKey("k"))
	assert.Equal(t, 0, countFifoEntries(t, s), "delete must remove the FIFO entry")
	_, ok = readFifoBackref(t, s, "k")
	assert.False(t, ok, "delete must remove the back-reference")
}

// TestFIFOEvictionSkipsSupersededDuplicateEntry proves the back-reference check:
// a stale duplicate FIFO entry (as a concurrent overwrite could leave) must be
// reclaimed rather than evicting its live key at the stale entry's old position.
func TestFIFOEvictionSkipsSupersededDuplicateEntry(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	// "victim" is written first (oldest real write); "k" second (newest).
	require.NoError(t, s.Put("victim", bytes.NewReader(bytes.Repeat([]byte("v"), 100)), 0))
	require.NoError(t, s.Put("k", bytes.NewReader(bytes.Repeat([]byte("k"), 100)), 0))

	// Inject a stale duplicate entry for "k", older than everything, WITHOUT
	// updating k's back-reference — exactly what a concurrent overwrite (or a
	// failed back-ref lookup during Put) would leave behind.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	staleEntry := keys.MakeFifoIndexKey("k", time.Now().Add(-time.Hour))
	require.NoError(t, s.meta.Handle().Put(wo, staleEntry, []byte{}))
	require.Equal(t, 3, countFifoEntries(t, s), "victim + k + stale-k")

	// Evict ~one key's worth. Oldest-first, the scan hits k's stale entry first.
	s.cleaner.evictByIndex(fifoEvictionIndex(), 50)

	// The stale entry is reclaimed; "victim" (the real oldest write) is evicted;
	// "k" survives — its stale entry must NOT have evicted it.
	_, foundK, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.True(t, foundK, "k must survive: a stale duplicate entry must not evict the live key")
	_, foundVictim, err := s.Get("victim", 0, 0)
	require.NoError(t, err)
	assert.False(t, foundVictim, "victim (oldest real write) should be evicted")

	assert.Equal(t, 1, countFifoEntries(t, s), "stale + victim entries gone; only k's current entry remains")
}

// TestFIFOEvictionEvictsWhenBackrefAbsent verifies that if a live key's FIFO
// entry has no back-reference at all, eviction evicts the key via that entry
// rather than deleting the entry and stranding the key (which would make it
// permanently un-evictable and defeat the disk cap).
func TestFIFOEvictionEvictsWhenBackrefAbsent(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader(bytes.Repeat([]byte("k"), 100)), 0))
	require.Equal(t, 1, countFifoEntries(t, s))

	// Remove the back-reference, leaving a live key with a FIFO entry but no
	// back-reference.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Delete(wo, keys.MakeFifoBackrefKey("k")))
	_, ok := readFifoBackref(t, s, "k")
	require.False(t, ok, "back-reference should be absent")

	s.cleaner.evictByIndex(fifoEvictionIndex(), 1<<30)

	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.False(t, found, "key with a live FIFO entry but no back-reference must be evicted, not stranded")
	assert.Equal(t, 0, countFifoEntries(t, s), "the entry should be reclaimed on eviction")
}

// TestLRUEvictionSkipsSupersededDuplicateEntry is the LRU counterpart to the FIFO
// test above: an overwrite leaves an orphan access-bucket entry (putLow does not
// delete the previous one), and the eviction scan must not evict the live key via
// that stale entry.
func TestLRUEvictionSkipsSupersededDuplicateEntry(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyLRU,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	// "victim" written first, "k" second (both current entries + secondary index).
	require.NoError(t, s.Put("victim", bytes.NewReader(bytes.Repeat([]byte("v"), 100)), 0))
	require.NoError(t, s.Put("k", bytes.NewReader(bytes.Repeat([]byte("k"), 100)), 0))

	// Inject a stale duplicate access-bucket entry for "k", older than everything,
	// WITHOUT repointing k's secondary index — what an overwrite leaves behind.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	staleEntry := keys.MakeBucketedAccessKey("k", time.Now().Add(-time.Hour))
	require.NoError(t, s.meta.Handle().Put(wo, staleEntry, []byte{}))
	require.Equal(t, 3, countAccessBucketEntries(t, s), "victim + k + stale-k")

	// Evict ~one key's worth. Oldest-first, the scan hits k's stale entry first.
	s.cleaner.evictByIndex(lruEvictionIndex(), 50)

	_, foundK, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.True(t, foundK, "k must survive: a stale duplicate access entry must not evict the live key")
	_, foundVictim, err := s.Get("victim", 0, 0)
	require.NoError(t, err)
	assert.False(t, foundVictim, "victim (the genuinely oldest current entry) should be evicted")

	assert.Equal(t, 1, countAccessBucketEntries(t, s), "stale + victim entries gone; only k's current entry remains")
}

// TestFIFOEvictionReadDoesNotProtect is the end-to-end contrast to TestLRUEviction:
// under FIFO, reading the oldest-written keys does not save them — they are still
// evicted first, while the newest-written keys survive.
func TestFIFOEvictionReadDoesNotProtect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fifo-evict-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         tmpDir,
		InlineThreshold:  100,
		CompactThreshold: 1024 * 1024,
		SegmentSize:      256 * 1024 * 1024,
		FdCacheSize:      100,
		MaxDiskUsage:     1000, // 1KB
		EvictionPolicy:   EvictionPolicyFIFO,
		CleanupInterval:  100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	// Oldest-written keys first (natural write order == FIFO order).
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("old-key-%d", i)
		require.NoError(t, s.Put(key, bytes.NewReader(bytes.Repeat([]byte("x"), 100)), 0))
	}

	time.Sleep(200 * time.Millisecond)

	// Repeatedly read the three oldest keys. Under LRU this would protect them;
	// under FIFO it must not.
	readKeys := []string{"old-key-0", "old-key-1", "old-key-2"}
	for r := 0; r < 5; r++ {
		for _, k := range readKeys {
			_, found, err := s.Get(k, 0, 0)
			require.NoError(t, err)
			require.True(t, found)
		}
	}

	// Add newest-written keys to push over the cap and trigger eviction.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("new-key-%d", i)
		require.NoError(t, s.Put(key, bytes.NewReader(bytes.Repeat([]byte("y"), 100)), 0))
	}

	require.Eventually(t, func() bool {
		_, evicted := s.CleanerStats()
		return evicted > 0
	}, 3*time.Second, 100*time.Millisecond, "expected eviction to occur")
	time.Sleep(300 * time.Millisecond)

	remaining := map[string]bool{}
	keyList, err := s.ListKeys("")
	require.NoError(t, err)
	for _, k := range keyList {
		remaining[k] = true
	}

	for _, k := range readKeys {
		assert.False(t, remaining[k], "%s was read but FIFO must still evict it (oldest-written)", k)
	}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("new-key-%d", i)
		assert.True(t, remaining[k], "%s is newest-written and should survive FIFO eviction", k)
	}
}
