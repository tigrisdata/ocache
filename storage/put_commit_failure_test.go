// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawFilesOnDisk lists the raw files currently in the storage's files/ directory.
func rawFilesOnDisk(t *testing.T, s *Storage) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.diskPath, "files"))
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestPut_MetadataCommitFailureReclaimsRawFile pins the orphan source from
// issue #156 that lives in Put itself: the raw file is written before the
// metadata row is committed, and when that commit fails nothing referenced the
// file. It must be reclaimed, not left behind.
func TestPut_MetadataCommitFailureReclaimsRawFile(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	injected := errors.New("injected metadata write failure")
	s.beforeMetaCommit = func() error { return injected }

	value := bytes.Repeat([]byte("x"), 2*1024) // above the 1 KiB inline threshold: a raw file
	err := s.Put("k", bytes.NewReader(value), 0)
	require.Error(t, err, "a failed metadata commit must surface to the caller")
	s.beforeMetaCommit = nil

	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.False(t, found, "a put whose commit failed must not be readable")
	assert.Zero(t, s.TotalSize(), "a failed put must not enter the cap's accounting")

	// The file is reclaimed through the deletion queue, which runs in the
	// background; it must be gone, not merely queued.
	assert.Eventually(t, func() bool { return len(rawFilesOnDisk(t, s)) == 0 },
		10*time.Second, 50*time.Millisecond,
		"raw file written before the failed commit was left behind as an orphan: %v", rawFilesOnDisk(t, s))
}

// TestPut_MetadataCommitSucceedsKeepsRawFile is the control: the hook is a
// pass-through when it returns nil, and a normal raw-file put keeps its file.
func TestPut_MetadataCommitSucceedsKeepsRawFile(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	calls := 0
	s.beforeMetaCommit = func() error { calls++; return nil }
	value := bytes.Repeat([]byte("y"), 2*1024)
	require.NoError(t, s.Put("k", bytes.NewReader(value), 0))
	assert.Equal(t, 1, calls)

	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Len(t, rawFilesOnDisk(t, s), 1)
}
