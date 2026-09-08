// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// Fenced deletes (issue #267): a CAS delete's tombstone carries the delete's
// stamp, an absent read hands that stamp (or a fresh one) out as an
// observation token, and a put ordered with a token from before the delete
// loses to it. These tests are the TAG scenarios the design exists for.

func absentToken(t *testing.T, s *Storage, key string) uint64 {
	t.Helper()
	_, ver, found, err := s.GetWithVersion(key)
	require.NoError(t, err)
	require.False(t, found, "%s must read as absent", key)
	require.NotZero(t, ver, "an absent read hands out an observation token, never 0")
	return ver
}

func mismatchWith(t *testing.T, err error) uint64 {
	t.Helper()
	vm, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok, "expected a version mismatch, got %v", err)
	return vm.CurrentVersion
}

// TestCAS_Fence_PopulateThatObservedAbsenceBeforeDeleteLoses: the invalidate-
// vs-populate race. A populate reads absence (token t), an invalidation lands
// on the still-missing key (fence S > t), and the populate's write carrying t
// is rejected with S. Refetching and retrying with S applies.
func TestCAS_Fence_PopulateThatObservedAbsenceBeforeDeleteLoses(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	tok := absentToken(t, s, "k")

	// Delete-if-absent on a missing key is not a no-op: it records the fence.
	require.NoError(t, s.DeleteIfVersion("k", 0))
	fence := absentToken(t, s, "k")
	assert.Greater(t, fence, tok, "the fence is stamped after the observation")

	// Pre-delete token loses, and is told the fence.
	_, err := s.PutIfVersion("k", bytes.NewReader([]byte("stale")), 0, tok)
	assert.Equal(t, fence, mismatchWith(t, err))
	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.False(t, found, "the stale populate must not have landed")

	// Refetch, retry with the fence: applies.
	v, err := s.PutIfVersion("k", bytes.NewReader([]byte("fresh")), 0, fence)
	require.NoError(t, err)
	r, ver, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v, ver)
	assert.Equal(t, "fresh", readAllString(t, r))

	// A later observation token also applies over a fence.
	require.NoError(t, s.DeleteIfVersion("k", v))
	later := absentToken(t, s, "k")
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("later")), 0, later)
	require.NoError(t, err)
}

// TestCAS_Fence_DoubleInvalidationOrdersRefill is TAG's invalidate-before-
// forward / invalidate-after-confirm pattern: v1 pre-forward, v2 post-confirm.
// A refill that observed v1 (and fetched pre-commit bytes) loses to v2; one
// that observed v2 wins. The unordered put-if-absent (0) still applies.
func TestCAS_Fence_DoubleInvalidationOrdersRefill(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	require.NoError(t, s.DeleteIfVersion("k", 0)) // v1: pre-forward
	v1 := absentToken(t, s, "k")
	require.NoError(t, s.DeleteIfVersion("k", 0)) // v2: post-confirm
	v2 := absentToken(t, s, "k")
	assert.Greater(t, v2, v1, "a second delete moves the fence forward")

	_, err := s.PutIfVersion("k", bytes.NewReader([]byte("pre-commit bytes")), 0, v1)
	assert.Equal(t, v2, mismatchWith(t, err), "a refill that observed only v1 must lose to v2")

	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("post-commit bytes")), 0, v2)
	require.NoError(t, err)
	r, _, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "post-commit bytes", readAllString(t, r))

	// A pre-fence token cannot move a fence either.
	_, cur, _, _ := s.GetWithVersion("k")
	require.NoError(t, s.DeleteIfVersion("k", cur))
	f := absentToken(t, s, "k")
	err = s.DeleteIfVersion("k", v1)
	assert.Equal(t, f, mismatchWith(t, err))
	// The fence token itself moves it.
	require.NoError(t, s.DeleteIfVersion("k", f))
	assert.Greater(t, absentToken(t, s, "k"), f)

	// put-if-absent stays the explicit unordered path.
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("unordered")), 0, 0)
	require.NoError(t, err)
}

// TestCAS_Fence_DeleteIfAbsentOverExpiredRowFencesAndReclaims: expiry itself
// is unfenced (an expired row reads with a fresh token, and 0 recreates), but
// a delete-if-absent over the expired row records a fence AND, because the
// tombstone is ref-less, owns the reclaim of the expired row's bytes.
func TestCAS_Fence_DeleteIfAbsentOverExpiredRowFencesAndReclaims(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	// A raw-file row that has expired, with a real file behind it.
	path := filepath.Join(s.diskPath, "files", "expired-raw")
	payload := bytes.Repeat([]byte("x"), 4096)
	require.NoError(t, os.WriteFile(path, payload, 0o644))
	row, err := proto.Marshal(&pb.ValueMessage{
		ValueType: pb.ValueType_RAW_FILE, RawFilePath: path, ValueLength: int64(len(payload)),
		Expiry: 2, Version: 777,
	})
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey("e"), row))
	s.cleaner.totalSize.Add(int64(len(payload)))

	tok := absentToken(t, s, "e")
	require.NoError(t, s.DeleteIfVersion("e", 0))
	fence := absentToken(t, s, "e")
	assert.Greater(t, fence, tok)

	assert.Zero(t, s.TotalSize(), "the delete accounts for the expired row it tombstoned")
	assert.Eventually(t, func() bool { _, err := os.Stat(path); return err != nil }, 10*time.Second, 50*time.Millisecond,
		"the expired row's file is reclaimed by the delete, not left for the sweep")

	_, err = s.PutIfVersion("e", bytes.NewReader([]byte("stale")), 0, tok)
	assert.Equal(t, fence, mismatchWith(t, err), "a populate that observed the expired row before the delete loses")
	_, err = s.PutIfVersion("e", bytes.NewReader([]byte("fresh")), 0, fence)
	require.NoError(t, err)
}

// TestCAS_Fence_RetainedBySweepThenAgedOut: the TTL sweep keeps a fence for
// the retention horizon and removes it after. Past the horizon the key has no
// history: any token applies (the documented ABA the horizon bounds).
func TestCAS_Fence_RetainedBySweepThenAgedOut(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	tok := absentToken(t, s, "k")
	require.NoError(t, s.DeleteIfVersion("k", 0))
	fence := absentToken(t, s, "k")

	s.cleaner.cleanupExpiredKeys()
	assert.Equal(t, fence, absentToken(t, s, "k"), "a fence younger than the retention horizon survives the sweep")
	_, err := s.PutIfVersion("k", bytes.NewReader([]byte("stale")), 0, tok)
	assert.Equal(t, fence, mismatchWith(t, err))

	s.cleaner.fenceRetention = 0 // age every fence out
	s.cleaner.cleanupExpiredKeys()
	after := absentToken(t, s, "k")
	assert.NotEqual(t, fence, after, "once aged out the key reads with a fresh token, not the fence")
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("old populate")), 0, tok)
	require.NoError(t, err, "past the horizon there is no history to order against")
}

// TestCAS_Fence_SweepKeepsFenceMovedInTheWindow: the sweep's pre-write
// re-check must compare a tombstone's stamp, not only its expiry. A fence
// moved forward between the scan and the write is a newer row and stays.
func TestCAS_Fence_SweepKeepsFenceMovedInTheWindow(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	require.NoError(t, s.DeleteIfVersion("k", 0))
	first := absentToken(t, s, "k")
	s.cleaner.fenceRetention = 0 // the scan will pick the fence up
	moved := false
	s.cleaner.beforeExpiryFlush = func() {
		if !moved {
			moved = true
			require.NoError(t, s.DeleteIfVersion("k", 0)) // moves the fence in the window
		}
	}
	s.cleaner.cleanupExpiredKeys()
	require.True(t, moved)

	second := absentToken(t, s, "k")
	assert.Greater(t, second, first, "the fence moved in the window must survive the sweep")
}

// TestCAS_DeleteIfVersion_DropsEvictionIndexEntries: a confirmed CAS delete
// removes the key's eviction-index entries, so a dead key does not sit at the
// head of the eviction order for the whole retention horizon.
func TestCAS_DeleteIfVersion_DropsEvictionIndexEntries(t *testing.T) {
	s, cleanup := createCASTestStorage(t) // disk cap set: LRU index active
	defer cleanup()
	require.Greater(t, s.cleaner.maxDiskUsage, int64(0))

	v, err := s.PutIfVersion("k", bytes.NewReader([]byte("data")), 0, 0)
	require.NoError(t, err)
	backref := keys.MakeBucketedAccessIndexKey("k")
	slice, err := s.meta.Handle().Get(putPointReadOpts, backref)
	require.NoError(t, err)
	require.True(t, slice.Exists(), "a won CAS put indexes the key for eviction")
	slice.Free()

	require.NoError(t, s.DeleteIfVersion("k", v))
	slice, err = s.meta.Handle().Get(putPointReadOpts, backref)
	require.NoError(t, err)
	assert.False(t, slice.Exists(), "a confirmed CAS delete drops the eviction-index entries")
	slice.Free()
}
