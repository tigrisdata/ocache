// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
