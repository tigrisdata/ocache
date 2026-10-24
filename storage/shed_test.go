// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Over the shed ratio the store reports it; without a cap or ratio it never does.
func TestOverShedRatio(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		TTL:             3600,
		InlineThreshold: 512,
		MaxDiskUsage:    4096,
		ShedDiskRatio:   1.0,
		CleanupInterval: time.Hour, // no eviction during the test
	})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	ratio, ok := s.DiskUsageRatio()
	require.True(t, ok)
	require.Zero(t, ratio)
	require.False(t, s.OverShedRatio())

	// 8 KiB into a 4 KiB cap: tracked usage is live on every write.
	require.NoError(t, s.Put("k1", bytes.NewReader(make([]byte, 8192)), 0))
	ratio, ok = s.DiskUsageRatio()
	require.True(t, ok)
	require.GreaterOrEqual(t, ratio, 1.0)
	require.True(t, s.OverShedRatio())

	// A plain Put is never refused by the store itself; the option lives in the caller.
	require.NoError(t, s.Put("k2", bytes.NewReader(make([]byte, 1024)), 0))
}

func TestOverShedRatio_DisabledWithoutCapOrRatio(t *testing.T) {
	noCap, err := NewStorageWithConfig(&StorageConfig{
		DiskPath: t.TempDir(), TTL: 3600, ShedDiskRatio: 1.0, CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { noCap.Close() })
	require.NoError(t, noCap.Put("k", bytes.NewReader(make([]byte, 8192)), 0))
	_, ok := noCap.DiskUsageRatio()
	require.False(t, ok)
	require.False(t, noCap.OverShedRatio())

	noRatio, err := NewStorageWithConfig(&StorageConfig{
		DiskPath: t.TempDir(), TTL: 3600, MaxDiskUsage: 1024, CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { noRatio.Close() })
	require.NoError(t, noRatio.Put("k", bytes.NewReader(make([]byte, 8192)), 0))
	ratio, ok := noRatio.DiskUsageRatio()
	require.True(t, ok)
	require.Greater(t, ratio, 1.0)
	require.False(t, noRatio.OverShedRatio())
}
