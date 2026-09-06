// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	pb "github.com/tigrisdata/ocache/storage/proto"
)

func createCASTestStorage(t *testing.T) (*Storage, func()) {
	// Cap set so eviction indexing runs; thresholds small enough to exercise the
	// raw-file path with modest payloads.
	return createTestStorage(t, 3600, 1024, 4*1024, 16*1024*1024, 1000, 1<<30)
}

func readAllString(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

func TestCAS_GetWithVersion_AbsentPutLegacy(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	// Absent → version 0.
	_, ver, found, err := s.GetWithVersion("missing")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, ver)

	// Plain Put stamps a real version.
	require.NoError(t, s.Put("k", bytes.NewReader([]byte("v1")), 0))
	r, ver, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Greater(t, ver, merge.VersionLegacy)
	assert.Equal(t, "v1", readAllString(t, r))

	// A hand-written pre-versioning row reports the legacy sentinel.
	legacy, err := proto.Marshal(&pb.ValueMessage{
		ValueType: pb.ValueType_INLINE, Data: []byte("old"), ValueLength: 3,
	})
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey("legacy-key"), legacy))

	_, ver, found, err = s.GetWithVersion("legacy-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, merge.VersionLegacy, ver)
}

func TestCAS_PutIfVersion_IfAbsentAndGuardedUpdate(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	// put-if-absent succeeds on a fresh key.
	v1, err := s.PutIfVersion("k", bytes.NewReader([]byte("first")), 0, 0)
	require.NoError(t, err)
	assert.NotZero(t, v1)

	// put-if-absent now fails, carrying the current version.
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("dupe")), 0, 0)
	vm, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok, "expected version mismatch, got %v", err)
	assert.Equal(t, v1, vm.CurrentVersion)

	// Guarded update with the right token succeeds and bumps the version.
	v2, err := s.PutIfVersion("k", bytes.NewReader([]byte("second")), 0, v1)
	require.NoError(t, err)
	assert.Greater(t, v2, v1)

	// The stale token now loses.
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("stale")), 0, v1)
	vm, ok = storageErrors.IsVersionMismatch(err)
	require.True(t, ok)
	assert.Equal(t, v2, vm.CurrentVersion)

	// Data reflects the winning writes only.
	r, ver, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v2, ver)
	assert.Equal(t, "second", readAllString(t, r))
}

func TestCAS_PlainPutShadowsAndInvalidatesTokens(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	v1, err := s.PutIfVersion("k", bytes.NewReader([]byte("cas")), 0, 0)
	require.NoError(t, err)

	// A plain Put (last-write-wins) shadows the row and issues a new stamp.
	require.NoError(t, s.Put("k", bytes.NewReader([]byte("plain")), 0))
	_, ver, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Greater(t, ver, v1)

	// The pre-plain-put token is dead.
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("stale")), 0, v1)
	vmErr, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok)
	assert.Equal(t, ver, vmErr.CurrentVersion)
}

func TestCAS_DeleteIfVersion_TokenModelAndRecreate(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	v1, err := s.PutIfVersion("k", bytes.NewReader([]byte("data")), 0, 0)
	require.NoError(t, err)

	// Wrong token loses; right token deletes.
	err = s.DeleteIfVersion("k", v1+999)
	_, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok)
	require.NoError(t, s.DeleteIfVersion("k", v1))

	// A deleted key reads as absent from both Get and GetWithVersion — CAS is
	// self-contained, so the tombstone does not hand out a recreate token.
	_, found, err := s.Get("k", 0, 0)
	require.NoError(t, err)
	assert.False(t, found)

	_, tombVer, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, tombVer, "a deleted key reads as absent (version 0), not a recreate token")

	// A stale token must NOT recreate over the tombstone...
	_, err = s.PutIfVersion("k", bytes.NewReader([]byte("wrong")), 0, v1)
	_, ok = storageErrors.IsVersionMismatch(err)
	require.True(t, ok)

	// ...put-if-absent does.
	v3, err := s.PutIfVersion("k", bytes.NewReader([]byte("reborn")), 0, 0)
	require.NoError(t, err)
	assert.Greater(t, v3, v1, "recreated keys always get a strictly higher version")

	r, ver, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v3, ver)
	assert.Equal(t, "reborn", readAllString(t, r))

	// Deleting an absent key mismatches with current 0.
	err = s.DeleteIfVersion("never-existed", 42)
	vmErr, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok)
	assert.Zero(t, vmErr.CurrentVersion)
}

func TestCAS_ParallelPutIfVersion_ExactlyOneWinner(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	v1, err := s.PutIfVersion("k", bytes.NewReader([]byte("base")), 0, 0)
	require.NoError(t, err)

	const contenders = 20
	var wins, mismatches atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.PutIfVersion("k", bytes.NewReader([]byte("contender")), 0, v1)
			switch {
			case err == nil:
				wins.Add(1)
			default:
				_, ok := storageErrors.IsVersionMismatch(err)
				if ok {
					mismatches.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), wins.Load(), "exactly one contender must win")
	assert.Equal(t, int64(contenders-1), mismatches.Load(), "all others must lose with a version mismatch")
}

func TestCAS_LargeValue_WinAndLose(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	big := bytes.Repeat([]byte("x"), 2048) // > inlineThreshold (1024) → raw file

	v1, err := s.PutIfVersion("big", bytes.NewReader(big), 0, 0)
	require.NoError(t, err)
	r, ver, found, err := s.GetWithVersion("big")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v1, ver)
	assert.Equal(t, string(big), readAllString(t, r))

	// A stale token is rejected by the sound fast-fail BEFORE anything is
	// spilled (stamps are unique, so a mismatch can never become a match).
	depthBefore := s.deletionQueue.GetQueueDepth()
	_, err = s.PutIfVersion("big", bytes.NewReader(big), 0, v1+999)
	_, ok := storageErrors.IsVersionMismatch(err)
	require.True(t, ok)
	assert.Equal(t, depthBefore, s.deletionQueue.GetQueueDepth(),
		"a fast-failed CAS must not have spilled anything")

	// Now force the true merge-loss path: the fast-fail passes (expected is
	// current at that moment), then a plain Put lands WHILE the body is still
	// streaming, so the already-spilled file loses at the merge and must be
	// reclaimed via the deletion queue. io.Pipe's synchronous writes give a
	// deterministic ordering: the second write cannot begin until the first is
	// fully consumed (fast-fail long past), and the interleaved Put completes
	// before the stream finishes.
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := s.PutIfVersion("big", pr, 0, v1)
		done <- err
	}()
	half := bytes.Repeat([]byte("y"), 1200) // > inlineThreshold: forces the raw-file path
	_, err = pw.Write(half)
	require.NoError(t, err)
	require.NoError(t, s.Put("big", bytes.NewReader([]byte("interloper")), 0)) // bumps the version
	_, err = pw.Write(half)
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	err = <-done
	_, ok = storageErrors.IsVersionMismatch(err)
	require.True(t, ok, "the CAS must lose at the merge, got %v", err)
	assert.Greater(t, s.deletionQueue.GetQueueDepth(), depthBefore,
		"the losing spill must be queued for deletion, not orphaned")

	// The interloper's value survived untouched.
	r2, _, found, err := s.GetWithVersion("big")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "interloper", readAllString(t, r2))
}

func TestCAS_OverwriteAccountingStaysExact(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	v1, err := s.PutIfVersion("k", bytes.NewReader(bytes.Repeat([]byte("a"), 500)), 0, 0)
	require.NoError(t, err)
	v2, err := s.PutIfVersion("k", bytes.NewReader(bytes.Repeat([]byte("b"), 300)), 0, v1)
	require.NoError(t, err)
	_ = v2

	// The running total must equal the live content, and agree with a rescan.
	assert.Equal(t, int64(300), s.cleaner.totalSize.Load(),
		"guarded overwrites must not accumulate replaced bytes")
	s.cleaner.calculateTotalSize()
	assert.Equal(t, int64(300), s.cleaner.totalSize.Load())
}

// TestCAS_TombstoneReadsAsAbsentAndRecreatesViaPutIfAbsent: a CAS-delete
// tombstone (Expiry==1 sentinel) reads as absent from GetWithVersion, and
// put-if-absent recreates over it. (Genuine TTL-expired rows behave the same
// through the read path's clock check.)
func TestCAS_TombstoneReadsAsAbsentAndRecreatesViaPutIfAbsent(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	// Hand-write a ref-less tombstone as a won CAS delete leaves behind.
	tomb, err := proto.Marshal(&pb.ValueMessage{Expiry: 1, Version: 12345})
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey("exp"), tomb))

	_, ver, found, err := s.GetWithVersion("exp")
	require.NoError(t, err)
	assert.False(t, found, "a tombstone reads as absent")
	assert.Zero(t, ver, "and exposes no recreate token")

	// put-if-absent recreates over it.
	v2, err := s.PutIfVersion("exp", bytes.NewReader([]byte("fresh")), 0, 0)
	require.NoError(t, err)
	assert.Greater(t, v2, uint64(12345), "recreated key gets a strictly higher stamp")

	r, _, found, err := s.GetWithVersion("exp")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "fresh", readAllString(t, r))
}

// TestCAS_DeleteReclaimsImmediately: a won DeleteIfVersion frees the backing
// bytes and size accounting right away (not one cleanup interval later), since
// the ref-less tombstone leaves nothing for the cleaner to reclaim.
func TestCAS_DeleteReclaimsImmediately(t *testing.T) {
	s, cleanup := createCASTestStorage(t)
	defer cleanup()

	big := bytes.Repeat([]byte("z"), 2048) // > inlineThreshold -> raw file
	v1, err := s.PutIfVersion("big", bytes.NewReader(big), 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(len(big)), s.cleaner.totalSize.Load())

	depthBefore := s.deletionQueue.GetQueueDepth()
	require.NoError(t, s.DeleteIfVersion("big", v1))

	assert.Zero(t, s.cleaner.totalSize.Load(),
		"a won CAS delete must decrement size immediately")
	assert.Greater(t, s.deletionQueue.GetQueueDepth(), depthBefore,
		"the backing raw file must be queued for deletion immediately, not left to the sweep")
}

// TestCAS_VersionsMonotonicAcrossRestart pins the durable-reservation
// guarantee: even if the wall clock at next startup is far behind previously
// issued stamps, no stamp is ever reused. Simulated by forcing the in-memory
// stamp source far into the future (as a fast clock would), issuing a stamp
// (which durably reserves past it), restarting, and asserting the next stamp
// still lands above it.
func TestCAS_VersionsMonotonicAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := func() *StorageConfig {
		return &StorageConfig{
			DiskPath:            dir,
			InlineThreshold:     1024,
			CompactThreshold:    4 * 1024,
			SegmentSize:         16 * 1024 * 1024,
			FdCacheSize:         1000,
			DisableRecompaction: true,
		}
	}

	s1, err := NewStorageWithConfig(cfg())
	require.NoError(t, err)

	farFuture := uint64(time.Now().Add(24 * time.Hour).UnixNano())
	s1.lastVersion.Store(farFuture)
	issued, err := s1.nextVersion()
	require.NoError(t, err)
	require.Greater(t, issued, farFuture)
	s1.Close()

	// Restart: the wall clock is ~24h behind the issued stamp, but the durable
	// reservation must keep new stamps strictly above it.
	s2, err := NewStorageWithConfig(cfg())
	require.NoError(t, err)
	defer s2.Close()

	next, err := s2.nextVersion()
	require.NoError(t, err)
	assert.Greater(t, next, issued,
		"stamps must stay monotonic across restarts regardless of the wall clock")
}
