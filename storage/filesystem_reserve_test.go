package storage

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskUsage_ReturnsPositive(t *testing.T) {
	free, total, ok := diskUsage(t.TempDir())
	require.True(t, ok, "statfs on a temp dir should succeed")
	assert.Greater(t, free, int64(0), "a writable temp dir should report free space")
	assert.GreaterOrEqual(t, total, free, "total capacity should be >= free")
}

// fakeUsage injects a controlled statfs result so the reserve logic can be tested
// without depending on the host's real free space.
func fakeUsage(free, total int64) func(string) (int64, int64, bool) {
	return func(string) (int64, int64, bool) { return free, total, true }
}

func newCappedStorage(t *testing.T, policy string) *Storage {
	t.Helper()
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    1 << 30, // logical cap far from breached
		EvictionPolicy:  policy,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	return s
}

// TestEnforceFilesystemReserve_EvictsBelowReserve: free below the reserve forces
// eviction even though the logical cap is nowhere near breached.
func TestEnforceFilesystemReserve_EvictsBelowReserve(t *testing.T) {
	for _, policy := range []string{EvictionPolicyLRU, EvictionPolicyFIFO} {
		t.Run(policy, func(t *testing.T) {
			s := newCappedStorage(t, policy)
			defer s.Close()

			const n = 10
			for i := 0; i < n; i++ {
				require.NoError(t, s.Put(fmt.Sprintf("key-%02d", i),
					bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
			}

			// free (256 MiB) below the 2 GiB reserve on a large (100 GiB) volume.
			// Each pass evicts a bounded slice, so run enough passes to drain.
			s.cleaner.diskUsageFn = fakeUsage(256<<20, 100<<30)
			for i := 0; i < 40; i++ {
				s.cleaner.enforceFilesystemReserve()
			}

			for i := 0; i < n; i++ {
				_, found, err := s.Get(fmt.Sprintf("key-%02d", i), 0, 0)
				require.NoError(t, err)
				assert.False(t, found, "backstop must evict when free space is below the reserve")
			}
		})
	}
}

// TestEnforceFilesystemReserve_NoEvictAboveReserve: a no-op when free is above the reserve.
func TestEnforceFilesystemReserve_NoEvictAboveReserve(t *testing.T) {
	s := newCappedStorage(t, EvictionPolicyLRU)
	defer s.Close()

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%02d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}

	// Plenty free (50 GiB) on a 100 GiB volume → above the 2 GiB reserve.
	s.cleaner.diskUsageFn = fakeUsage(50<<30, 100<<30)
	s.cleaner.enforceFilesystemReserve()

	for i := 0; i < n; i++ {
		_, found, err := s.Get(fmt.Sprintf("key-%02d", i), 0, 0)
		require.NoError(t, err)
		assert.True(t, found, "backstop must not evict when free space is above the reserve")
	}
}

// TestEnforceFilesystemReserve_BoundedSlicePerTick: one low reading must not evict
// the whole cache at once; a single pass evicts at most a slice (1/N of the cache),
// and successive ticks converge (no in-flight bookkeeping to stall on).
func TestEnforceFilesystemReserve_BoundedSlicePerTick(t *testing.T) {
	s := newCappedStorage(t, EvictionPolicyFIFO)
	defer s.Close()

	const n = 100
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%03d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}
	require.Equal(t, n, countFifoEntries(t, s))

	// Deficit is huge (free = 0, far below the reserve) but the per-tick slice is
	// capped at 1/reserveEvictSliceFraction of the cache, so one pass evicts only a
	// slice — not the whole cache.
	s.cleaner.diskUsageFn = fakeUsage(0, 100<<30)

	s.cleaner.enforceFilesystemReserve()
	afterFirst := countFifoEntries(t, s)
	require.Greater(t, afterFirst, n/2, "one low reading must not evict most of the cache")
	require.Less(t, afterFirst, n, "the pass should evict a slice")

	// Successive ticks keep reclaiming and converge toward the reserve.
	s.cleaner.enforceFilesystemReserve()
	require.Less(t, countFifoEntries(t, s), afterFirst, "successive ticks converge")
}

// TestEnforceFilesystemReserve_SmallVolumeCapsReserve: on a small volume the 2 GiB
// floor is capped to a fraction of capacity, so a healthy small disk is not evicted
// to empty.
func TestEnforceFilesystemReserve_SmallVolumeCapsReserve(t *testing.T) {
	s := newCappedStorage(t, EvictionPolicyLRU)
	defer s.Close()

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%02d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}

	// 4 GiB volume with 1 GiB free. The raw 2 GiB reserve would fire forever, but
	// capped at total/10 = ~410 MiB the reserve is satisfied → no eviction.
	s.cleaner.diskUsageFn = fakeUsage(1<<30, 4<<30)
	s.cleaner.enforceFilesystemReserve()

	for i := 0; i < n; i++ {
		_, found, err := s.Get(fmt.Sprintf("key-%02d", i), 0, 0)
		require.NoError(t, err)
		assert.True(t, found, "small-volume reserve cap must not evict a healthy disk")
	}
}

// TestEnforceFilesystemReserve_InactiveWhenStatfsUnavailable: a failed or
// implausible statfs reading (ok=false — including mounts reporting zero
// capacity, which diskUsage rejects) must leave the backstop inactive for the
// tick rather than read as "disk full" and wipe the cache.
func TestEnforceFilesystemReserve_InactiveWhenStatfsUnavailable(t *testing.T) {
	s := newCappedStorage(t, EvictionPolicyLRU)
	defer s.Close()

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%02d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}

	s.cleaner.diskUsageFn = func(string) (int64, int64, bool) { return 0, 0, false }
	s.cleaner.enforceFilesystemReserve()

	for i := 0; i < n; i++ {
		_, found, err := s.Get(fmt.Sprintf("key-%02d", i), 0, 0)
		require.NoError(t, err)
		assert.True(t, found, "an unavailable statfs reading must not trigger eviction")
	}
}

// TestEnforceFilesystemReserve_NoopWithoutCap: no eviction index without a cap.
func TestEnforceFilesystemReserve_NoopWithoutCap(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    0, // uncapped
		EvictionPolicy:  EvictionPolicyLRU,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v")), 0))

	s.cleaner.diskUsageFn = fakeUsage(0, 100<<30) // would breach, but no cap
	s.cleaner.enforceFilesystemReserve()

	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.True(t, found, "uncapped cache has no eviction index; backstop must be a no-op")
}
