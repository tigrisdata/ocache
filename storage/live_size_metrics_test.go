// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tigrisdata/ocache/common/metrics"
)

// TestTotalSizeTracksWritesAndDeletes verifies that Storage.TotalSize() reflects
// the live logical cache size (maintained on every write and delete) rather than
// only the value computed at startup, and that refreshSizeMetrics publishes that
// live total to the disk-usage gauge — the fix for issue #183.
func TestTotalSizeTracksWritesAndDeletes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "live-size-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Long cleanup interval so the background tick doesn't race the gauge
	// assertions below — we drive refreshSizeMetrics explicitly instead.
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         tmpDir,
		InlineThreshold:  1 << 20, // keep test values inline in RocksDB
		CompactThreshold: 16 << 20,
		SegmentSize:      256 << 20,
		FdCacheSize:      100,
		CleanupInterval:  time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()

	require.NotNil(t, s.cleaner)
	require.Equal(t, int64(0), s.TotalSize(), "empty store should have zero total size")

	val := bytes.Repeat([]byte("x"), 1000)
	require.NoError(t, s.Put("k1", bytes.NewReader(val), 0))
	require.NoError(t, s.Put("k2", bytes.NewReader(val), 0))

	// The live getter reflects writes immediately, without a rescan.
	assert.Equal(t, int64(2000), s.TotalSize())

	// refreshSizeMetrics (invoked on every cleaner tick) publishes the live
	// total to the gauge, so ocache_disk_usage_bytes tracks current contents.
	s.cleaner.refreshSizeMetrics()
	assert.Equal(t, float64(2000),
		testutil.ToFloat64(metrics.DiskUsageBytes.WithLabelValues("total")))

	// Deleting a key lowers the tracked total.
	require.NoError(t, s.DeleteKey("k1"))
	assert.Equal(t, int64(1000), s.TotalSize())

	s.cleaner.refreshSizeMetrics()
	assert.Equal(t, float64(1000),
		testutil.ToFloat64(metrics.DiskUsageBytes.WithLabelValues("total")))
}
