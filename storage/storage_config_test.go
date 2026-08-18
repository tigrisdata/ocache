// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewStorageWithConfig_CompactionBytesPerSecond verifies that the storage
// layer treats an unset budget as unthrottled — the 16 MiB/s default is a
// server-flag concern only, so library/embedded users are never silently
// throttled by an upgrade — while preserving an explicit limit.
func TestNewStorageWithConfig_CompactionBytesPerSecond(t *testing.T) {
	t.Run("unset stays unthrottled", func(t *testing.T) {
		config := &StorageConfig{DiskPath: t.TempDir(), DisableRecompaction: true}
		s, err := NewStorageWithConfig(config)
		require.NoError(t, err)
		defer s.Close()

		require.LessOrEqual(t, config.CompactionBytesPerSecond, int64(0),
			"storage must not clamp an unset budget to a throttling default")
	})

	t.Run("explicit value preserved", func(t *testing.T) {
		const limit = int64(2 * 1024 * 1024)
		config := &StorageConfig{
			DiskPath:                 t.TempDir(),
			DisableRecompaction:      true,
			CompactionBytesPerSecond: limit,
		}
		s, err := NewStorageWithConfig(config)
		require.NoError(t, err)
		defer s.Close()

		require.Equal(t, limit, config.CompactionBytesPerSecond)
	})
}

// TestNewStorageWithConfig_DeleteBatchSize verifies the deletion-queue batch
// size is configurable: an unset value falls back to DefaultDeleteBatchSize,
// and an explicit value is preserved. NewStorageWithConfig applies the default
// in place on the passed config, which is what these assertions observe.
func TestNewStorageWithConfig_DeleteBatchSize(t *testing.T) {
	t.Run("default applied when unset", func(t *testing.T) {
		config := &StorageConfig{DiskPath: t.TempDir(), DisableRecompaction: true}
		s, err := NewStorageWithConfig(config)
		require.NoError(t, err)
		defer s.Close()

		require.Equal(t, DefaultDeleteBatchSize, config.DeleteBatchSize)
	})

	t.Run("explicit value preserved", func(t *testing.T) {
		config := &StorageConfig{DiskPath: t.TempDir(), DisableRecompaction: true, DeleteBatchSize: 250}
		s, err := NewStorageWithConfig(config)
		require.NoError(t, err)
		defer s.Close()

		require.Equal(t, 250, config.DeleteBatchSize)
	})
}
