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
		{"to end", 5, 0, []byte("56789"), false},          // end=0 means read to EOF
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
