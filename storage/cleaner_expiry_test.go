// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// newExpiryTestStorage returns a storage whose cleaner never ticks on its own,
// so each test drives cleanupExpiredKeys directly.
func newExpiryTestStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: DefaultInlineThreshold,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

// writeExpiredInline hand-writes an already-expired inline row (Expiry = 2:
// in the past, and not the tombstone sentinel) and seeds the live total with
// its size, the state a row is in between its TTL passing and the next sweep.
func writeExpiredInline(t *testing.T, s *Storage, key, data string) {
	t.Helper()
	value, err := proto.Marshal(&pb.ValueMessage{
		ValueType:   pb.ValueType_INLINE,
		Data:        []byte(data),
		ValueLength: int64(len(data)),
		Expiry:      2,
	})
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), value))
	s.cleaner.totalSize.Add(int64(len(data)))
}

func readValue(t *testing.T, s *Storage, key string) (string, bool) {
	t.Helper()
	r, found, err := s.Get(key, 0, 0)
	require.NoError(t, err)
	if !found {
		return "", false
	}
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	if c, ok := r.(io.Closer); ok {
		require.NoError(t, c.Close())
	}
	return string(data), true
}

// onceBeforeFlush arms the cleaner's test hook to run fn exactly once, in the
// window between the TTL scan and the deletion batch's re-check and write.
func onceBeforeFlush(s *Storage, fn func()) {
	var once sync.Once
	s.cleaner.beforeExpiryFlush = func() { once.Do(fn) }
}

// TestCleanerExpiry_PutBeforeFlushSurvives pins issue #256 for a plain put: a
// value written after the scan saw the key expired, but before the deletion
// batch is written, must survive the sweep. The sweep must also not subtract
// its bytes — the put already accounted for the row it replaced.
func TestCleanerExpiry_PutBeforeFlushSurvives(t *testing.T) {
	s := newExpiryTestStorage(t)
	writeExpiredInline(t, s, "k", "stale")
	onceBeforeFlush(s, func() {
		require.NoError(t, s.Put("k", bytes.NewReader([]byte("fresh")), 0))
	})

	s.cleaner.cleanupExpiredKeys()

	got, found := readValue(t, s, "k")
	require.True(t, found, "the put that landed before the deletion batch was shadowed by the sweep")
	assert.Equal(t, "fresh", got)
	cleaned, _ := s.CleanerStats()
	assert.Zero(t, cleaned, "a rewritten key must not count as cleaned")
	assert.Equal(t, int64(len("fresh")), s.TotalSize(), "the sweep must not subtract bytes the put already accounted for")
}

// TestCleanerExpiry_CASRecreateBeforeFlushSurvives is the acknowledged-write
// case from #256: put-if-absent over the expired row returns a version, and
// that version must still be there after the sweep.
func TestCleanerExpiry_CASRecreateBeforeFlushSurvives(t *testing.T) {
	s := newExpiryTestStorage(t)
	writeExpiredInline(t, s, "k", "stale")
	var version uint64
	onceBeforeFlush(s, func() {
		v, err := s.PutIfVersion("k", bytes.NewReader([]byte("fresh")), 0, 0)
		require.NoError(t, err)
		version = v
	})

	s.cleaner.cleanupExpiredKeys()

	r, got, found, err := s.GetWithVersion("k")
	require.NoError(t, err)
	require.True(t, found, "the acknowledged CAS recreate was shadowed by the sweep")
	assert.Equal(t, version, got)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "fresh", string(data))
}

// TestCleanerExpiry_UnracedRowStillDeleted: the re-check must not weaken the
// sweep. A key rewritten in the window is kept; one that is not is deleted and
// accounted for exactly once.
func TestCleanerExpiry_UnracedRowStillDeleted(t *testing.T) {
	s := newExpiryTestStorage(t)
	writeExpiredInline(t, s, "raced", "stale")
	writeExpiredInline(t, s, "unraced", "stale-too")
	onceBeforeFlush(s, func() {
		require.NoError(t, s.Put("raced", bytes.NewReader([]byte("fresh")), 0))
	})

	s.cleaner.cleanupExpiredKeys()

	_, found := readValue(t, s, "unraced")
	assert.False(t, found, "an expired key nobody rewrote must be deleted")
	got, found := readValue(t, s, "raced")
	require.True(t, found)
	assert.Equal(t, "fresh", got)
	cleaned, _ := s.CleanerStats()
	assert.Equal(t, int64(1), cleaned)
	assert.Equal(t, int64(len("fresh")), s.TotalSize())
}
