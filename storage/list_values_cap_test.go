// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListKeyValuesWithPagination_OmitsOversizeValues verifies that List-with-
// values returns small values inline but omits (without reading from disk) any
// value larger than MaxListValueSize, reporting its size instead (#165).
func TestListKeyValuesWithPagination_OmitsOversizeValues(t *testing.T) {
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         t.TempDir(),
		InlineThreshold:  1024,              // >1KB → not inline
		CompactThreshold: 512 * 1024,        // >512KB → large raw file (not compacted)
		SegmentSize:      256 * 1024 * 1024, // keep compact < segment
		CleanupInterval:  0,                 // don't sweep during the test
	})
	require.NoError(t, err)
	defer s.Close()

	small := []byte("small-value")
	large := bytes.Repeat([]byte("x"), int(MaxListValueSize)+1) // > 1 MiB
	require.NoError(t, s.Put("a-small", bytes.NewReader(small), 0))
	require.NoError(t, s.Put("b-large", bytes.NewReader(large), 0))

	entries, _, _, err := s.ListKeyValuesWithPagination("", "", 100)
	require.NoError(t, err)

	got := make(map[string]KeyValue, len(entries))
	for _, e := range entries {
		got[e.Key] = e
	}

	require.Contains(t, got, "a-small")
	assert.False(t, got["a-small"].ValueOmitted, "small value must not be omitted")
	assert.Equal(t, small, got["a-small"].Value)

	require.Contains(t, got, "b-large")
	assert.True(t, got["b-large"].ValueOmitted, "value over the cap must be omitted")
	assert.Nil(t, got["b-large"].Value, "omitted value must not be buffered")
	assert.Equal(t, int64(len(large)), got["b-large"].ValueLength, "size must still be reported")
}

// TestNewStorageWithConfig_ClampsCompactThresholdBelowSegmentSize verifies the
// startup guardrail clamps an invalid CompactThreshold (>= SegmentSize) rather
// than failing to start (#165).
func TestNewStorageWithConfig_ClampsCompactThresholdBelowSegmentSize(t *testing.T) {
	const segmentSize = int64(256 * 1024 * 1024)
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         t.TempDir(),
		CompactThreshold: 512 * 1024 * 1024, // 512 MiB >= 256 MiB segment → clamp
		SegmentSize:      segmentSize,
		CleanupInterval:  0,
	})
	require.NoError(t, err)
	defer s.Close()

	assert.Equal(t, segmentSize-1, s.compactThreshold,
		"compact-threshold should be clamped to just below segment-size")
}
