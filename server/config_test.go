// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig_CompactionBytesPerSecond(t *testing.T) {
	originalLimit := *compactionBytesPerSecond
	originalConfig := AppConfig
	t.Cleanup(func() {
		*compactionBytesPerSecond = originalLimit
		AppConfig = originalConfig
	})

	const limit = int64(3 * 1024 * 1024)
	*compactionBytesPerSecond = limit
	LoadConfig()

	require.Equal(t, limit, AppConfig.CompactionBytesPerSecond)
}
