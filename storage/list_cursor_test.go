// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaginationCursorAfterEmptyKey(t *testing.T) {
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	values := map[string][]byte{
		"":     []byte("empty"),
		"\x00": []byte("nul"),
		"a":    []byte("a"),
		"p:a":  []byte("prefixed"),
	}
	for key, value := range values {
		require.NoError(t, s.Put(key, bytes.NewReader(value), 0))
	}

	initial, _, hasMore, err := s.ListKeysWithPagination("", "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{""}, initial)
	require.True(t, hasMore)

	emptyCursor := ""
	keys, lastKey, hasMore, err := s.ListKeysWithPaginationCursor("", &emptyCursor, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"\x00"}, keys)
	require.Equal(t, "\x00", lastKey)
	require.True(t, hasMore)

	nulCursor := "\x00"
	keys, lastKey, hasMore, err = s.ListKeysWithPaginationCursor("", &nulCursor, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, keys)
	require.Equal(t, "a", lastKey)
	require.True(t, hasMore)

	entries, lastKey, hasMore, err := s.ListKeyValuesWithPaginationCursor("", &emptyCursor, 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "\x00", entries[0].Key)
	require.Equal(t, values["\x00"], entries[0].Value)
	require.Equal(t, "\x00", lastKey)
	require.True(t, hasMore)

	entries, lastKey, hasMore, err = s.ListKeyValuesWithPaginationCursor("p:", &emptyCursor, 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "p:a", entries[0].Key)
	require.Equal(t, values["p:a"], entries[0].Value)
	require.Empty(t, lastKey)
	require.False(t, hasMore)
}
