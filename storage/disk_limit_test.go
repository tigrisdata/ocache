// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cap enforcement is continuous and incremental (#298): it starts below the
// cap at the high-water mark and evicts a bounded slice per tick toward the
// low-water mark, re-measuring between ticks. These tests pin that shape with
// a small cap of 1 KiB objects (one object = 1% of the cap), under FIFO so the
// eviction order needs no access-time setup.

const diskLimitTestCap = 100 * 1024 // 100 KiB: one 1 KiB object is 1% of the cap

func newDiskLimitStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		MaxDiskUsage:    diskLimitTestCap,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour, // enforcement is driven by the tests, not the loop
	})
	require.NoError(t, err)
	return s
}

func putDiskLimitKeys(t *testing.T, s *Storage, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("key-%03d", i),
			bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), 0))
	}
}

// TestEnforceDiskLimit_StartsBelowCap: a total between the high-water mark
// and the cap is evicted down to the low-water mark, before the cap is reached.
func TestEnforceDiskLimit_StartsBelowCap(t *testing.T) {
	s := newDiskLimitStorage(t)
	defer s.Close()

	putDiskLimitKeys(t, s, 98) // 98% of the cap: over the 97% high-water mark
	require.Equal(t, int64(98*1024), s.cleaner.TotalSize())

	s.cleaner.enforceDiskLimit()

	total := s.cleaner.TotalSize()
	assert.LessOrEqual(t, total, int64(float64(diskLimitTestCap)*capEvictLowWater),
		"one tick must bring the total to the low-water mark")
	assert.GreaterOrEqual(t, total, int64(90*1024),
		"a steady-state slice is small: only the excess over the low-water mark goes")
	assert.Equal(t, int(total/1024), countFifoEntries(t, s), "index entries track the evicted keys")
}

// TestEnforceDiskLimit_NoEvictBelowHighWater: under the high-water mark nothing
// is evicted, even though a cap is set.
func TestEnforceDiskLimit_NoEvictBelowHighWater(t *testing.T) {
	s := newDiskLimitStorage(t)
	defer s.Close()

	putDiskLimitKeys(t, s, 96) // 96% of the cap: under the 97% high-water mark

	s.cleaner.enforceDiskLimit()

	assert.Equal(t, int64(96*1024), s.cleaner.TotalSize(), "no eviction below the high-water mark")
	assert.Equal(t, 96, countFifoEntries(t, s))
}

// TestEnforceDiskLimit_BoundedSlicePerTick: a total far over the cap is not
// brought down in one pass; each tick evicts at most capEvictMaxSliceFraction
// of the cap and successive ticks converge to the low-water mark.
func TestEnforceDiskLimit_BoundedSlicePerTick(t *testing.T) {
	s := newDiskLimitStorage(t)
	defer s.Close()

	const n = 200 // 200% of the cap
	putDiskLimitKeys(t, s, n)
	require.Equal(t, int64(n*1024), s.cleaner.TotalSize())

	s.cleaner.enforceDiskLimit()
	afterFirst := countFifoEntries(t, s)
	maxSliceKeys := diskLimitTestCap / int(capEvictMaxSliceFraction) / 1024 // 10 keys
	require.GreaterOrEqual(t, afterFirst, n-maxSliceKeys, "one tick evicts at most the maximum slice")
	require.Less(t, afterFirst, n, "the tick evicts something")

	// The total fell with the committed batch, before any further tick.
	require.Equal(t, int64(afterFirst*1024), s.cleaner.TotalSize(), "the live total tracks the committed evictions")

	// Successive ticks converge to the low-water mark and then stop.
	for i := 0; i < 40; i++ {
		s.cleaner.enforceDiskLimit()
	}
	total := s.cleaner.TotalSize()
	lowWater := int64(float64(diskLimitTestCap) * capEvictLowWater)
	assert.LessOrEqual(t, total, lowWater, "ticks converge to the low-water mark")
	assert.Greater(t, total, lowWater-int64(maxSliceKeys*1024), "and do not keep evicting once there")
	assert.Equal(t, int(total/1024), countFifoEntries(t, s))
}

// TestEnforceDiskLimit_OvershootWithinSlice: a burst that is within the
// per-tick bound is worked off in one tick, straight to the low-water mark.
func TestEnforceDiskLimit_OvershootWithinSlice(t *testing.T) {
	s := newDiskLimitStorage(t)
	defer s.Close()

	putDiskLimitKeys(t, s, 105) // 5% over the cap, 10% over the low-water mark

	s.cleaner.enforceDiskLimit()

	total := s.cleaner.TotalSize()
	assert.LessOrEqual(t, total, int64(float64(diskLimitTestCap)*capEvictLowWater), "one tick reaches the low-water mark")
	assert.GreaterOrEqual(t, total, int64(94*1024), "and evicts no more than that")
}

// TestEnforceDiskLimit_NoopWithoutCap: with no cap there is nothing to enforce.
func TestEnforceDiskLimit_NoopWithoutCap(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: 1 << 20,
		EvictionPolicy:  EvictionPolicyFIFO,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	putDiskLimitKeys(t, s, 10)
	s.cleaner.enforceDiskLimit()
	assert.Equal(t, int64(10*1024), s.cleaner.TotalSize())
}
