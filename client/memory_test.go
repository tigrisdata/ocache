// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMemoryCache_PutAndGet(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put data
	key := "test-key"
	data := []byte("test-value")
	err := cache.Put(ctx, key, data, 0)
	require.NoError(t, err)

	// Get data
	result, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestMemoryCache_GetNotFound(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	_, err := cache.Get(ctx, "non-existent-key")
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestMemoryCache_Delete(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put data
	key := "delete-me"
	err := cache.Put(ctx, key, []byte("data"), 0)
	require.NoError(t, err)

	// Delete
	err = cache.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deleted
	_, err = cache.Get(ctx, key)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestMemoryCache_DeleteNotFound(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	err := cache.Delete(ctx, "non-existent")
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestMemoryCache_TTLExpiration(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "ttl-key"
	data := []byte("expires-soon")

	// Put with 1 second TTL
	err := cache.Put(ctx, key, data, 1)
	require.NoError(t, err)

	// Should be accessible immediately
	result, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, result)

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Should be expired now
	_, err = cache.Get(ctx, key)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestMemoryCache_NoTTL(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "no-ttl"
	data := []byte("permanent")

	// Put with 0 TTL (no expiration)
	err := cache.Put(ctx, key, data, 0)
	require.NoError(t, err)

	// Should remain accessible
	result, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestMemoryCache_List(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put multiple keys
	require.NoError(t, cache.Put(ctx, "prefix/a", []byte("1"), 0))
	require.NoError(t, cache.Put(ctx, "prefix/b", []byte("2"), 0))
	require.NoError(t, cache.Put(ctx, "prefix/c", []byte("3"), 0))
	require.NoError(t, cache.Put(ctx, "other/x", []byte("4"), 0))

	// List with prefix
	keys, err := cache.List(ctx, "prefix/")
	require.NoError(t, err)
	assert.Equal(t, []string{"prefix/a", "prefix/b", "prefix/c"}, keys)

	// List all
	keys, err = cache.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, keys, 4)
}

func TestMemoryCache_ListPage(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put multiple keys
	for i := 0; i < 10; i++ {
		key := "key" + string(rune('a'+i))
		require.NoError(t, cache.Put(ctx, key, []byte("data"), 0))
	}

	// First page
	keys, nextToken, hasMore, err := cache.ListPage(ctx, "", 3, "")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.True(t, hasMore)
	assert.NotEmpty(t, nextToken)

	// Second page
	keys, nextToken, hasMore, err = cache.ListPage(ctx, "", 3, nextToken)
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.True(t, hasMore)

	// Continue until no more
	for hasMore {
		keys, nextToken, hasMore, err = cache.ListPage(ctx, "", 3, nextToken)
		require.NoError(t, err)
	}
}

func TestMemoryCache_ListPageWithValues(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put multiple keys with distinct values
	for i := 0; i < 10; i++ {
		key := "key" + string(rune('a'+i))
		require.NoError(t, cache.Put(ctx, key, []byte("value-"+key), 0))
	}

	// First page
	entries, nextToken, hasMore, err := cache.ListPageWithValues(ctx, "", 3, "")
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.True(t, hasMore)
	assert.NotEmpty(t, nextToken)

	// Verify keys are sorted and values match
	for i := 1; i < len(entries); i++ {
		assert.LessOrEqual(t, entries[i-1].Key, entries[i].Key)
	}
	for _, e := range entries {
		assert.Equal(t, []byte("value-"+e.Key), e.Value)
	}

	// Second page
	entries2, nextToken2, hasMore2, err := cache.ListPageWithValues(ctx, "", 3, nextToken)
	require.NoError(t, err)
	assert.Len(t, entries2, 3)
	assert.True(t, hasMore2)

	// Verify no overlap with first page
	firstKeys := make(map[string]bool)
	for _, e := range entries {
		firstKeys[e.Key] = true
	}
	for _, e := range entries2 {
		assert.False(t, firstKeys[e.Key], "key %s should not appear in second page", e.Key)
		assert.Equal(t, []byte("value-"+e.Key), e.Value)
	}

	// Continue until no more
	token := nextToken2
	for hasMore2 {
		entries2, token, hasMore2, err = cache.ListPageWithValues(ctx, "", 3, token)
		require.NoError(t, err)
		for _, e := range entries2 {
			assert.Equal(t, []byte("value-"+e.Key), e.Value)
		}
	}

	// Prefix filter
	entries, _, _, err = cache.ListPageWithValues(ctx, "keya", 10, "")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "keya", entries[0].Key)
	assert.Equal(t, []byte("value-keya"), entries[0].Value)

	// Empty results
	entries, nextToken, hasMore, err = cache.ListPageWithValues(ctx, "zzz", 10, "")
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Empty(t, nextToken)
	assert.False(t, hasMore)
}

func TestMemoryCache_GetRange(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "range-key"
	data := []byte("0123456789")
	require.NoError(t, cache.Put(ctx, key, data, 0))

	tests := []struct {
		name      string
		start     int64
		end       int64
		expected  []byte
		expectErr bool
	}{
		{"full range", 0, 9, []byte("0123456789"), false}, // inclusive: bytes 0-9
		{"partial range", 2, 4, []byte("234"), false},     // inclusive: bytes 2-4
		{"from start", 0, 4, []byte("01234"), false},      // inclusive: bytes 0-4
		{"to end", 5, 0, []byte("56789"), false},          // end <= 0 means read to EOF
		{"invalid start", 20, 25, nil, true},              // start beyond data
		{"single byte", 5, 5, []byte("5"), false},         // inclusive: byte 5 only
		{"invalid range start > end", 5, 3, nil, true},    // invalid: start > end
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cache.GetRange(ctx, key, tt.start, tt.end)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMemoryCache_GetRangeNotFound(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	_, err := cache.GetRange(ctx, "missing", 0, 9) // inclusive: bytes 0-9
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestMemoryCache_PutStream(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "stream-put"
	data := []byte("streamed data content")
	reader := bytes.NewReader(data)

	err := cache.PutStream(ctx, key, reader, 0)
	require.NoError(t, err)

	result, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestMemoryCache_GetStream(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "stream-get"
	data := []byte("data to stream out")
	require.NoError(t, cache.Put(ctx, key, data, 0))

	var buf bytes.Buffer
	err := cache.GetStream(ctx, key, &buf)
	require.NoError(t, err)
	assert.Equal(t, data, buf.Bytes())
}

func TestMemoryCache_GetRangeStream(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "range-stream"
	data := []byte("0123456789")
	require.NoError(t, cache.Put(ctx, key, data, 0))

	var buf bytes.Buffer
	err := cache.GetRangeStream(ctx, key, 2, 6, &buf) // inclusive: bytes 2-6
	require.NoError(t, err)
	assert.Equal(t, []byte("23456"), buf.Bytes())
}

func TestMemoryCache_ConcurrentAccess(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "concurrent-key"
			data := []byte("value")
			_ = cache.Put(ctx, key, data, 0)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cache.Get(ctx, "concurrent-key")
		}()
	}

	wg.Wait()

	// Verify cache is still functional
	err := cache.Put(ctx, "final-key", []byte("final"), 0)
	require.NoError(t, err)

	result, err := cache.Get(ctx, "final-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("final"), result)
}

func TestMemoryCache_ContextCancellation(t *testing.T) {
	cache := NewMemoryCache()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := cache.Put(ctx, "key", []byte("data"), 0)
	assert.Error(t, err)

	_, err = cache.Get(ctx, "key")
	assert.Error(t, err)

	err = cache.Delete(ctx, "key")
	assert.Error(t, err)

	_, err = cache.List(ctx, "")
	assert.Error(t, err)
}

func TestMemoryCache_DataIsolation(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put data
	key := "isolation-key"
	originalData := []byte("original")
	require.NoError(t, cache.Put(ctx, key, originalData, 0))

	// Modify original data after Put
	originalData[0] = 'X'

	// Get should return unmodified data
	result, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, byte('o'), result[0])

	// Modify returned data
	result[0] = 'Y'

	// Get again should still return original
	result2, err := cache.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, byte('o'), result2[0])
}

func TestMemoryCache_Close(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	require.NoError(t, cache.Put(ctx, "key1", []byte("data1"), 0))
	require.NoError(t, cache.Put(ctx, "key2", []byte("data2"), 0))

	err := cache.Close()
	require.NoError(t, err)

	// After close, cache should be empty
	_, err = cache.Get(ctx, "key1")
	require.Error(t, err)
}

func TestMemoryCache_GetMode(t *testing.T) {
	cache := NewMemoryCache()
	assert.Equal(t, ModeSimple, cache.GetMode())
}

func TestMemoryCache_GetConnectedNodes(t *testing.T) {
	cache := NewMemoryCache()
	nodes := cache.GetConnectedNodes()
	assert.Equal(t, []string{"memory"}, nodes)
}

func TestMemoryCache_ListExcludesExpired(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	// Put one permanent, one expiring
	require.NoError(t, cache.Put(ctx, "permanent", []byte("data"), 0))
	require.NoError(t, cache.Put(ctx, "expiring", []byte("data"), 1))

	// Initially both visible
	keys, err := cache.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, keys, 2)

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Only permanent should be visible
	keys, err = cache.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"permanent"}, keys)
}

func TestMemoryCache_OverwriteKey(t *testing.T) {
	cache := NewMemoryCache()
	ctx := context.Background()

	key := "overwrite-me"
	require.NoError(t, cache.Put(ctx, key, []byte("first"), 0))

	result, _ := cache.Get(ctx, key)
	assert.Equal(t, []byte("first"), result)

	require.NoError(t, cache.Put(ctx, key, []byte("second"), 0))

	result, _ = cache.Get(ctx, key)
	assert.Equal(t, []byte("second"), result)
}

// TestMemoryCache_CAS_MatchesStorageSemantics locks in the storage-aligned CAS
// behavior of the test double: a live plain-written key reports the legacy
// version (not 0), so put-if-absent is rejected against it — mirroring real
// storage, so a test using MemoryCache cannot pass on behavior production
// rejects.
func TestMemoryCache_CAS_MatchesStorageSemantics(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache()

	// A plain-written live key reads as the legacy version, never 0.
	require.NoError(t, cache.Put(ctx, "plain", []byte("v"), 0))
	_, ver, found, err := cache.GetWithVersion(ctx, "plain")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, memoryLegacyVersion, ver)

	// put-if-absent must be REJECTED against the live plain key (matches storage).
	_, err = cache.PutIfVersion(ctx, "plain", []byte("clobber"), 0, 0)
	vm, ok := IsVersionMismatch(err)
	require.True(t, ok, "expected mismatch, got %v", err)
	assert.Equal(t, memoryLegacyVersion, vm.CurrentVersion)

	// A CAS create stamps a version strictly above the legacy sentinel.
	v1, err := cache.PutIfVersion(ctx, "cas", []byte("a"), 0, 0)
	require.NoError(t, err)
	assert.Greater(t, v1, memoryLegacyVersion)

	// Guarded update chain: right token wins and bumps; stale token loses.
	v2, err := cache.PutIfVersion(ctx, "cas", []byte("b"), 0, v1)
	require.NoError(t, err)
	assert.Greater(t, v2, v1)
	_, err = cache.PutIfVersion(ctx, "cas", []byte("c"), 0, v1)
	_, ok = IsVersionMismatch(err)
	require.True(t, ok)

	// DeleteIfVersion: wrong token rejected, right token deletes; then absent.
	require.Error(t, cache.DeleteIfVersion(ctx, "cas", v1))
	require.NoError(t, cache.DeleteIfVersion(ctx, "cas", v2))
	_, _, found, err = cache.GetWithVersion(ctx, "cas")
	require.NoError(t, err)
	assert.False(t, found)
}

// TestMemoryCache_CAS_PlainWritesDemoteVersion: the test double must never be
// MORE permissive than real storage. A plain (non-CAS) Put over a CAS'd key
// stores no stamp, so the key must read as the legacy version and a stale CAS
// token must be rejected; a plain Delete, a lazy TTL expiry and Close must all
// make the key absent (version 0) and must not let the old stamp resurface on a
// later plain re-create. (Mixing plain and CAS writes on one key is unsupported,
// but "never accept a stale token" is the invariant storage keeps regardless.)
func TestMemoryCache_CAS_PlainWritesDemoteVersion(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache()

	// Plain Put over a CAS'd key: demoted to legacy, old token rejected.
	v1, err := cache.PutIfVersion(ctx, "k", []byte("a"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, cache.Put(ctx, "k", []byte("plain"), 0))
	_, ver, found, err := cache.GetWithVersion(ctx, "k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, memoryLegacyVersion, ver, "a plain Put must not keep the old CAS stamp")
	_, err = cache.PutIfVersion(ctx, "k", []byte("stale"), 0, v1)
	vm, ok := IsVersionMismatch(err)
	require.True(t, ok, "stale token must be rejected after a plain Put, got %v", err)
	assert.Equal(t, memoryLegacyVersion, vm.CurrentVersion)

	// Plain Delete: absent, and a plain re-create reads as legacy, not the stamp.
	v2, err := cache.PutIfVersion(ctx, "d", []byte("a"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, cache.Delete(ctx, "d"))
	_, ver, found, err = cache.GetWithVersion(ctx, "d")
	require.NoError(t, err)
	assert.False(t, found)
	assert.NotZero(t, ver, "absent reads hand out an observation token (#267)")
	require.NoError(t, cache.Put(ctx, "d", []byte("again"), 0))
	_, ver, _, err = cache.GetWithVersion(ctx, "d")
	require.NoError(t, err)
	assert.Equal(t, memoryLegacyVersion, ver, "old stamp must not resurface on a plain re-create")
	_, err = cache.PutIfVersion(ctx, "d", []byte("stale"), 0, v2)
	_, ok = IsVersionMismatch(err)
	require.True(t, ok)

	// Lazy TTL expiry: absent (0), and the expiry sweep must drop the stamp too.
	v3, err := cache.PutIfVersion(ctx, "e", []byte("a"), 0, 0)
	require.NoError(t, err)
	cache.mu.Lock()
	ent := cache.data["e"]
	ent.expiresAt = time.Now().Add(-time.Second)
	cache.data["e"] = ent
	cache.mu.Unlock()
	_, ver, found, err = cache.GetWithVersion(ctx, "e")
	require.NoError(t, err)
	assert.False(t, found)
	assert.NotZero(t, ver, "absent reads hand out an observation token (#267)")
	_, err = cache.Get(ctx, "e") // triggers the lazy-expiry drop
	require.Error(t, err)
	require.NoError(t, cache.Put(ctx, "e", []byte("again"), 0))
	_, ver, _, err = cache.GetWithVersion(ctx, "e")
	require.NoError(t, err)
	assert.Equal(t, memoryLegacyVersion, ver)
	_, err = cache.PutIfVersion(ctx, "e", []byte("stale"), 0, v3)
	_, ok = IsVersionMismatch(err)
	require.True(t, ok)

	// Close clears versions along with data.
	_, err = cache.PutIfVersion(ctx, "c", []byte("a"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, cache.Close())
	_, ver, found, err = cache.GetWithVersion(ctx, "c")
	require.NoError(t, err)
	assert.False(t, found)
	assert.NotZero(t, ver, "absent reads hand out an observation token (#267)")
}

// TestMemoryCache_CAS_Fences mirrors storage's fenced deletes (issue #267): an
// absent read hands out an observation token; a CAS delete on a missing or
// dead key records a fence; a put carrying a token from before the fence
// loses and is told the fence; the fence token (or a later one, or 0)
// recreates; a second delete moves the fence forward.
func TestMemoryCache_CAS_Fences(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache()

	// A populate observes absence...
	_, tok, found, err := cache.GetWithVersion(ctx, "k")
	require.NoError(t, err)
	require.False(t, found)
	require.NotZero(t, tok)
	// ...an invalidation lands on the still-missing key...
	require.NoError(t, cache.DeleteIfVersion(ctx, "k", 0))
	_, fence, found, err := cache.GetWithVersion(ctx, "k")
	require.NoError(t, err)
	require.False(t, found)
	assert.Greater(t, fence, tok, "the fence is stamped after the observation")
	// ...and the populate's write, carrying its pre-delete token, loses.
	_, err = cache.PutIfVersion(ctx, "k", []byte("stale"), 0, tok)
	vm, ok := IsVersionMismatch(err)
	require.True(t, ok, "a pre-fence token must lose, got %v", err)
	assert.Equal(t, fence, vm.CurrentVersion, "the mismatch carries the fence to retry with")

	// Second invalidation moves the fence; a token from between them loses too.
	require.NoError(t, cache.DeleteIfVersion(ctx, "k", 0))
	_, fence2, _, err := cache.GetWithVersion(ctx, "k")
	require.NoError(t, err)
	assert.Greater(t, fence2, fence)
	_, err = cache.PutIfVersion(ctx, "k", []byte("between"), 0, fence)
	_, ok = IsVersionMismatch(err)
	require.True(t, ok)

	// The current fence token recreates; the fence is consumed.
	v, err := cache.PutIfVersion(ctx, "k", []byte("fresh"), 0, fence2)
	require.NoError(t, err)
	data, ver, found, err := cache.GetWithVersion(ctx, "k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v, ver)
	assert.Equal(t, []byte("fresh"), data)

	// put-if-absent (0) stays the unordered escape hatch over a fence.
	require.NoError(t, cache.DeleteIfVersion(ctx, "k", v))
	_, err = cache.PutIfVersion(ctx, "k", []byte("unordered"), 0, 0)
	require.NoError(t, err)

	// A guarded delete with a pre-fence token cannot move the fence.
	_, cur, _, _ := cache.GetWithVersion(ctx, "k")
	require.NoError(t, cache.DeleteIfVersion(ctx, "k", cur))
	_, f3, _, _ := cache.GetWithVersion(ctx, "k")
	err = cache.DeleteIfVersion(ctx, "k", cur)
	vm, ok = IsVersionMismatch(err)
	require.True(t, ok)
	assert.Equal(t, f3, vm.CurrentVersion)

	// Plain ops drop the fence: mixing plain and CAS ops on a key voids the
	// guarantee, exactly as a plain overwrite replaces the tombstone in storage.
	require.NoError(t, cache.Put(ctx, "k", []byte("plain"), 0))
	require.NoError(t, cache.Delete(ctx, "k"))
	_, tok2, _, _ := cache.GetWithVersion(ctx, "k")
	assert.NotEqual(t, f3, tok2, "after plain ops the key has no fence, just a fresh token")
}
