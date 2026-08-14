package storage

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tigrisdata/ocache/storage/keys"
)

// TestBackfillEvictionIndex_MakesPreexistingKeysEvictable reproduces the #189
// invisible-key state: keys written while uncapped get no eviction-index entry,
// so after a cap is later configured enforceDiskLimit finds zero candidates and
// the cap can never be met (the state that filled Goldsky's disks into the
// terminal ENOSPC of #204). The startup backfill must index those pre-existing
// keys so eviction can reclaim them, on both policies.
func TestBackfillEvictionIndex_MakesPreexistingKeysEvictable(t *testing.T) {
	for _, policy := range []string{EvictionPolicyLRU, EvictionPolicyFIFO} {
		t.Run(policy, func(t *testing.T) {
			dir := t.TempDir()
			const n = 20

			// Phase 1: write keys with NO cap → putLow writes no index entries.
			s1, err := NewStorageWithConfig(&StorageConfig{
				DiskPath:        dir,
				InlineThreshold: 1 << 20,
				MaxDiskUsage:    0, // uncapped
				EvictionPolicy:  policy,
				CleanupInterval: time.Hour,
			})
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				require.NoError(t, s1.Put(fmt.Sprintf("key-%02d", i),
					bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
			}
			s1.Close()

			// Phase 2: reopen the SAME directory WITH a cap. Backfill runs at init.
			s2, err := NewStorageWithConfig(&StorageConfig{
				DiskPath:        dir,
				InlineThreshold: 1 << 20,
				MaxDiskUsage:    1 << 20,
				EvictionPolicy:  policy,
				CleanupInterval: time.Hour,
			})
			require.NoError(t, err)
			defer s2.Close()

			// A clean backfill must not leave the fast-retry flag set.
			require.False(t, s2.cleaner.backfillPending.Load(),
				"a successful backfill must clear backfillPending")

			// FIFO exposes a per-key entry count: backfill must create exactly one
			// entry per key (idempotent, no duplicates).
			if policy == EvictionPolicyFIFO {
				require.Equal(t, n, countFifoEntries(t, s2),
					"backfill must index every pre-existing key exactly once")
			}

			// Evict everything. Without backfill this finds zero candidates and the
			// keys survive; with backfill every key is reclaimed.
			idx := lruEvictionIndex()
			if policy == EvictionPolicyFIFO {
				idx = fifoEvictionIndex()
			}
			s2.cleaner.evictByIndex(idx, 1<<30)

			for i := 0; i < n; i++ {
				_, found, err := s2.Get(fmt.Sprintf("key-%02d", i), 0, 0)
				require.NoError(t, err)
				assert.False(t, found, "pre-existing key must be evictable after backfill")
			}
		})
	}
}

// TestBackfillEvictionIndex_LeavesAlreadyIndexedKeysAlone confirms the backfill
// is idempotent: keys written under a cap already have exactly one FIFO entry,
// and a reopen (which re-runs the backfill) must not add a duplicate.
func TestBackfillEvictionIndex_LeavesAlreadyIndexedKeysAlone(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	cfg := func() *StorageConfig {
		return &StorageConfig{
			DiskPath:        dir,
			InlineThreshold: 1 << 20,
			MaxDiskUsage:    1 << 20,
			EvictionPolicy:  EvictionPolicyFIFO,
			CleanupInterval: time.Hour,
		}
	}

	s1, err := NewStorageWithConfig(cfg())
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, s1.Put(fmt.Sprintf("key-%02d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}
	require.Equal(t, n, countFifoEntries(t, s1))
	s1.Close()

	// Reopen: every key is already indexed, so backfill must be a no-op.
	s2, err := NewStorageWithConfig(cfg())
	require.NoError(t, err)
	defer s2.Close()
	require.Equal(t, n, countFifoEntries(t, s2),
		"reopening a fully-indexed cache must not duplicate FIFO entries")
}

// TestReconcile_HourlyPathSelfHealsMissingCoverage verifies coverage is
// self-healing: the periodic reconciliation (calculateTotalSize) also backfills,
// so a live key that lost its eviction-index entry (e.g. from a backfill left
// incomplete by an error) is re-indexed on the next pass rather than staying
// invisible until a restart.
func TestReconcile_HourlyPathSelfHealsMissingCoverage(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%02d", i), bytes.NewReader([]byte("v")), 0))
	}
	require.Equal(t, n, countFifoEntries(t, s))

	// Strip one key's FIFO entry + back-reference, leaving it live but unindexed.
	fifoKey, ok := readFifoBackref(t, s, "key-02")
	require.True(t, ok)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Delete(wo, []byte(fifoKey)))
	require.NoError(t, s.meta.Handle().Delete(wo, keys.MakeFifoBackrefKey("key-02")))
	require.Equal(t, n-1, countFifoEntries(t, s))

	// A reconciliation pass must repair the missing coverage.
	s.cleaner.calculateTotalSize()
	require.Equal(t, n, countFifoEntries(t, s),
		"reconciliation must re-index a live key that lost its eviction-index entry")
	_, ok = readFifoBackref(t, s, "key-02")
	require.True(t, ok, "the repaired key must have a back-reference again")
}

// TestBackfill_SkipsOrphanBackReferences exercises the merge-join's handling of
// back-reference rows with no metadata row: orphans sorting both before and after
// the real keys must be skipped without derailing the backfill of the real keys.
func TestBackfill_SkipsOrphanBackReferences(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	s1, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        dir,
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    0, // uncapped → real keys written without index entries
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, s1.Put(fmt.Sprintf("key-%02d", i), bytes.NewReader([]byte("v")), 0))
	}
	// Inject orphan back-references (no metadata row) sorting before and after the
	// real "key-NN" range, so the merge-join hits both the skip-past-orphan and the
	// hold-orphan-for-later branches.
	wo := grocksdb.NewDefaultWriteOptions()
	require.NoError(t, s1.meta.Handle().Put(wo, keys.MakeFifoBackrefKey("aaa-orphan"), []byte("x")))
	require.NoError(t, s1.meta.Handle().Put(wo, keys.MakeFifoBackrefKey("zzz-orphan"), []byte("x")))
	wo.Destroy()
	s1.Close()

	s2, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        dir,
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s2.Close()

	// Every real key got exactly one FIFO entry despite the orphan back-references.
	require.Equal(t, n, countFifoEntries(t, s2))
	s2.cleaner.evictByIndex(fifoEvictionIndex(), 1<<30)
	for i := 0; i < n; i++ {
		_, found, err := s2.Get(fmt.Sprintf("key-%02d", i), 0, 0)
		require.NoError(t, err)
		assert.False(t, found, "real key must be evictable after backfill despite orphan back-references")
	}
}
