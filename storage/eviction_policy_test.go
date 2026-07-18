// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

// readAccessIndex returns the current bucketed access key recorded for a user
// key via the secondary index, or ("", false) if none exists. The bucketed key
// encodes the access timestamp, so a change means the entry was re-bucketed.
func readAccessIndex(t *testing.T, s *Storage, key string) (string, bool) {
	t.Helper()
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	idxKey := keys.MakeBucketedAccessIndexKey(key)
	slice, err := s.meta.Handle().Get(ro, idxKey)
	require.NoError(t, err)
	defer slice.Free()
	if !slice.Exists() {
		return "", false
	}
	return string(slice.Data()), true
}

// TestEvictionPolicyNormalization checks that the policy is defaulted/validated
// and that the access updater (which exists only to refresh recency on reads) is
// created for LRU but not for FIFO.
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
			assert.Equal(t, tc.wantUpdater, s.accessUpdater != nil,
				"accessUpdater presence should track LRU-with-cap")
		})
	}
}

// TestEvictionPolicyReadBumpBehavior verifies the core mechanism: in LRU a read
// re-buckets the entry (refreshing recency), while in FIFO a read leaves the
// entry frozen at its write-time bucket.
func TestEvictionPolicyReadBumpBehavior(t *testing.T) {
	newStore := func(policy string) *Storage {
		s, err := NewStorageWithConfig(&StorageConfig{
			DiskPath:          t.TempDir(),
			InlineThreshold:   1 << 20,
			MaxDiskUsage:      1 << 20, // enable eviction bookkeeping (access index)
			EvictionPolicy:    policy,
			AccessUpdateDelay: time.Millisecond, // don't time-gate the read bump
			CleanupInterval:   time.Hour,
		})
		require.NoError(t, err)
		return s
	}

	t.Run("lru bumps on read", func(t *testing.T) {
		s := newStore(EvictionPolicyLRU)
		defer s.Close()

		require.NoError(t, s.Put("k", bytes.NewReader([]byte("value")), 0))
		before, ok := readAccessIndex(t, s, "k")
		require.True(t, ok)

		_, _, err := s.Get("k", 0, 0)
		require.NoError(t, err)
		s.FlushAccessUpdates()

		after, ok := readAccessIndex(t, s, "k")
		require.True(t, ok)
		assert.NotEqual(t, before, after, "LRU read should re-bucket the entry")
	})

	t.Run("fifo does not bump on read", func(t *testing.T) {
		s := newStore(EvictionPolicyFIFO)
		defer s.Close()

		require.NoError(t, s.Put("k", bytes.NewReader([]byte("value")), 0))
		before, ok := readAccessIndex(t, s, "k")
		require.True(t, ok)

		// Multiple reads must not change the entry's write-time bucket.
		for i := 0; i < 3; i++ {
			_, _, err := s.Get("k", 0, 0)
			require.NoError(t, err)
		}
		s.FlushAccessUpdates() // no-op in FIFO (updater is nil); harmless

		after, ok := readAccessIndex(t, s, "k")
		require.True(t, ok)
		assert.Equal(t, before, after, "FIFO read must leave the write-time bucket unchanged")
	})
}

// countAccessBucketEntries returns the number of access-bucket entries currently
// stored (one is expected per live key).
func countAccessBucketEntries(t *testing.T, s *Storage) int {
	t.Helper()
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()
	prefix := GetOldestAccessBucketPrefix()
	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		n++
		it.Key().Free()
		it.Value().Free()
	}
	return n
}

// TestPutOverwriteReplacesAccessEntry verifies that overwriting a key does not
// leave orphan access-bucket entries: each overwrite removes the previous entry
// so exactly one remains per live key. Orphans would otherwise make a rewritten
// key look old to the eviction scan and grow the index with total writes.
func TestPutOverwriteReplacesAccessEntry(t *testing.T) {
	for _, policy := range []string{EvictionPolicyLRU, EvictionPolicyFIFO} {
		t.Run(policy, func(t *testing.T) {
			s, err := NewStorageWithConfig(&StorageConfig{
				DiskPath:        t.TempDir(),
				InlineThreshold: 1 << 20,
				MaxDiskUsage:    1 << 20,
				EvictionPolicy:  policy,
				CleanupInterval: time.Hour,
			})
			require.NoError(t, err)
			defer s.Close()

			require.NoError(t, s.Put("k", bytes.NewReader([]byte("v0")), 0))
			require.Equal(t, 1, countAccessBucketEntries(t, s))

			for i := 0; i < 5; i++ {
				require.NoError(t, s.Put("k", bytes.NewReader([]byte(fmt.Sprintf("v%d", i+1))), 0))
			}

			assert.Equal(t, 1, countAccessBucketEntries(t, s),
				"overwrites must not leave orphan access-bucket entries")

			// The surviving entry is the current one (matches the secondary index).
			idx, ok := readAccessIndex(t, s, "k")
			require.True(t, ok)
			ro := metadata.CreateReadOptions(true, false)
			defer ro.Destroy()
			it := s.meta.Handle().NewIterator(ro)
			defer it.Close()
			prefix := GetOldestAccessBucketPrefix()
			it.Seek(prefix)
			require.True(t, it.ValidForPrefix(prefix))
			assert.Equal(t, idx, string(it.Key().Data()),
				"the single access entry should be the one the secondary index points to")
		})
	}
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

	// Let the initial size baseline settle.
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

	// Wait for eviction to run.
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

	// The read oldest keys were NOT protected — they should be evicted.
	for _, k := range readKeys {
		assert.False(t, remaining[k], "%s was read but FIFO must still evict it (oldest-written)", k)
	}
	// The newest-written keys survive.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("new-key-%d", i)
		assert.True(t, remaining[k], "%s is newest-written and should survive FIFO eviction", k)
	}
}
