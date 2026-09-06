// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
)

// CAS integration tests (issue #254). Unlike the storage-package unit tests,
// these exercise the conditional operations against the FULL running stack —
// the real compactor, TTL cleaner, and deletion queue — where the hardest
// review-surfaced bugs lived (resurrection via in-flight compaction, recreate
// after TTL expiry, spill reclamation for large objects under contention).

// casStorage returns the underlying *storage.Storage; CAS ops are storage-layer
// only, so they are not on the harness interface.
func (s *CASSuite) casStorage() *storage.Storage {
	sa, ok := s.Harness.(TestStorageAccess)
	require.True(s.T(), ok, "single-node harness must expose direct storage access")
	stor, ok := sa.GetStorage().(*storage.Storage)
	require.True(s.T(), ok)
	return stor
}

// casRead returns a key's value bytes and version via GetWithVersion.
func casRead(t require.TestingT, stor *storage.Storage, key string) (data []byte, version uint64, found bool) {
	r, version, found, err := stor.GetWithVersion(key)
	require.NoError(t, err)
	if !found {
		return nil, version, false
	}
	data, err = io.ReadAll(r)
	require.NoError(t, err)
	if rc, ok := r.(io.ReadCloser); ok {
		_ = rc.Close()
	}
	return data, version, true
}

// Test_CAS_DeletedKeyStaysDeletedAcrossCompaction is the end-to-end guard for
// the resurrection bug: CAS-deleting medium raw-file keys while the real
// compactor migrates them raw->segment must not let a stale migration operand
// revive a deleted key.
func (s *CASSuite) Test_CAS_DeletedKeyStaysDeletedAcrossCompaction() {
	t := s.T()
	stor := s.casStorage()

	const n = 24
	// 8KB: above the 1KB inline threshold (raw file) and below the 1MB compact
	// threshold, so the compactor will migrate it to a segment.
	type entry struct {
		key string
		ver uint64
	}
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("cas-del-compact-%d", i)
		v, err := stor.PutIfVersion(key, bytes.NewReader(GenerateRandomData(8*1024)), 0, 0)
		require.NoError(t, err)
		entries = append(entries, entry{key, v})
	}

	// Delete every key via CAS concurrently, while compaction cycles (the
	// suite's RecompactionInterval is sub-second). Collect errors off the test
	// goroutine — testify require must not FailNow from a worker.
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i, e := range entries {
		wg.Add(1)
		go func(i int, e entry) {
			defer wg.Done()
			errs[i] = stor.DeleteIfVersion(e.key, e.ver)
		}(i, e)
	}
	wg.Wait()
	for i, err := range errs {
		// Migration preserves the version, so a concurrent compaction cannot
		// make the delete lose; every delete must succeed.
		require.NoError(t, err, "delete of %s", entries[i].key)
	}

	// Give the compactor several cycles to run against the tombstoned keys; a
	// resurrection would revive one of them.
	time.Sleep(2 * time.Second)
	for _, e := range entries {
		_, err := s.Harness.GetObject(e.key)
		require.Error(t, err, "deleted key %s was resurrected", e.key)
		_, _, found := casRead(t, stor, e.key)
		require.False(t, found, "deleted key %s reads as present via CAS", e.key)
	}
}

// Test_CAS_RecreateAfterTTLExpiry exercises the recreate-over-expired path
// against the real cleaner: a key that TTL-expires and is swept must be
// recreatable with put-if-absent.
func (s *CASSuite) Test_CAS_RecreateAfterTTLExpiry() {
	t := s.T()
	stor := s.casStorage()
	key := "cas-ttl-recreate"

	v1, err := stor.PutIfVersion(key, bytes.NewReader([]byte("first")), 1, 0) // 1s TTL
	require.NoError(t, err)
	require.NotZero(t, v1)

	// Wait for the cleaner to sweep the expired row (suite CleanupInterval is
	// sub-second); the extra margin ensures this key specifically is gone.
	waiter, ok := s.Harness.(TestWaitForCleanup)
	require.True(t, ok)
	require.NoError(t, waiter.WaitForCleanup(5*time.Second))
	require.Eventually(t, func() bool {
		_, err := s.Harness.GetObject(key)
		return err != nil
	}, 5*time.Second, 100*time.Millisecond, "key should be swept after TTL")

	// GetWithVersion reports it absent (version 0) — no recreate token handout.
	_, ver, found := casRead(t, stor, key)
	require.False(t, found)
	require.Zero(t, ver)

	// Put-if-absent recreates over the swept key.
	v2, err := stor.PutIfVersion(key, bytes.NewReader([]byte("second")), 0, 0)
	require.NoError(t, err)
	require.Greater(t, v2, v1, "recreated key gets a strictly higher stamp")

	data, err := s.Harness.GetObject(key)
	require.NoError(t, err)
	require.Equal(t, "second", string(data))
}

// Test_CAS_ContendedPutReclaimsLoserSpills verifies that under contention on a
// large (permanent raw-file) value, exactly one CAS put wins and every loser's
// spilled file is reclaimed through the real deletion queue — no orphans.
func (s *CASSuite) Test_CAS_ContendedPutReclaimsLoserSpills() {
	t := s.T()
	stor := s.casStorage()
	key := "cas-contended-large"

	// 2MB: above the 1MB compact threshold, so it stays a permanent raw file and
	// is never migrated to a segment — making the on-disk raw-file count a clean
	// measure of orphaned spills.
	big := func() *bytes.Reader { return bytes.NewReader(GenerateRandomData(2 * 1024 * 1024)) }

	v1, err := stor.PutIfVersion(key, big(), 0, 0)
	require.NoError(t, err)

	const contenders = 8
	var wins, mismatches int64
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := stor.PutIfVersion(key, big(), 0, v1)
			if err == nil {
				atomic.AddInt64(&wins, 1)
				return
			}
			if _, ok := storageErrors.IsVersionMismatch(err); ok {
				atomic.AddInt64(&mismatches, 1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), atomic.LoadInt64(&wins), "exactly one contender must win")
	require.Equal(t, int64(contenders-1), atomic.LoadInt64(&mismatches), "all others must lose with a version mismatch")

	// The winner and initial value are the same key (one live raw file); every
	// loser spilled a 2MB file that must be queued and deleted. The on-disk raw
	// file count must converge to exactly 1.
	require.Eventually(t, func() bool {
		return s.Harness.GetStorageStats().RawFileCount == 1
	}, 15*time.Second, 250*time.Millisecond, "loser spills must be reclaimed; exactly one raw file should remain")
}

// Test_CAS_GuardedOverwriteReclaimsReplaced runs a guarded-overwrite chain on a
// permanent raw-file value: each win must reclaim the replaced file (bounded
// disk, no accumulation) and invalidate the previous token.
func (s *CASSuite) Test_CAS_GuardedOverwriteReclaimsReplaced() {
	t := s.T()
	stor := s.casStorage()
	key := "cas-overwrite-chain"
	big := func() *bytes.Reader { return bytes.NewReader(GenerateRandomData(2 * 1024 * 1024)) }

	v, err := stor.PutIfVersion(key, big(), 0, 0)
	require.NoError(t, err)

	const rounds = 10
	for i := 0; i < rounds; i++ {
		nv, err := stor.PutIfVersion(key, big(), 0, v)
		require.NoError(t, err, "round %d", i)
		require.Greater(t, nv, v)

		// The now-stale token must lose.
		_, err = stor.PutIfVersion(key, big(), 0, v)
		_, ok := storageErrors.IsVersionMismatch(err)
		require.True(t, ok, "stale token must be rejected at round %d", i)
		v = nv
	}

	// Every replaced (and every losing) raw file must be reclaimed: exactly one
	// live raw file remains, and the tracked size reflects one value, not 30+.
	require.Eventually(t, func() bool {
		return s.Harness.GetStorageStats().RawFileCount == 1
	}, 15*time.Second, 250*time.Millisecond, "replaced raw files must be reclaimed")
	require.LessOrEqual(t, stor.TotalSize(), int64(4*1024*1024),
		"guarded overwrites must not accumulate replaced bytes")
}
