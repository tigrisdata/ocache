// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentLiveIndexKeyRoundTripAndOffsetOrder(t *testing.T) {
	path := "/tmp/segments/with spaces/segment_α.seg"
	offsets := []int64{4096, 0, 128}
	encoded := make([][]byte, 0, len(offsets))
	for _, offset := range offsets {
		encoded = append(encoded, MakeSegmentLiveIndexKey(path, offset))
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })
	for i, want := range []int64{0, 128, 4096} {
		gotPath, gotOffset, ok := ParseSegmentLiveIndexKey(encoded[i])
		require.True(t, ok)
		require.Equal(t, path, gotPath)
		require.Equal(t, want, gotOffset)
	}
}

func TestSegmentLiveIndexEntryEncoding(t *testing.T) {
	want := SegmentLiveIndexEntry{
		Key:           "object-key",
		ValueLength:   1234,
		HeaderSize:    28,
		Checksum:      0x12345678,
		HeaderVersion: 1,
	}
	encoded, err := EncodeSegmentLiveIndexEntry(want)
	require.NoError(t, err)
	got, err := DecodeSegmentLiveIndexEntry(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	encoded[0]++
	_, err = DecodeSegmentLiveIndexEntry(encoded)
	require.Error(t, err)
}

func TestSegmentLiveCoverageEncoding(t *testing.T) {
	want := SegmentLiveIndexCoverage{Entries: 17, DataBytes: 987654, Size: 1234567}
	encoded, err := EncodeSegmentLiveIndexCoverage(want)
	require.NoError(t, err)
	got, err := DecodeSegmentLiveIndexCoverage(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
