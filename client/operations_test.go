// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/client/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestPut_BasicOperations tests basic Put functionality
func TestPut_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("put small object", func(t *testing.T) {
		key := "test-small"
		value := []byte("hello world")
		err := client.Put(ctx, key, value, 0)
		require.NoError(t, err)

		// Verify it was stored
		assert.Equal(t, value, server.cacheService.data[key])
	})

	t.Run("put with TTL", func(t *testing.T) {
		key := "test-ttl"
		value := []byte("with ttl")
		ttl := int64(3600)
		err := client.Put(ctx, key, value, ttl)
		require.NoError(t, err)

		// Verify TTL was set
		assert.Equal(t, ttl, server.cacheService.ttls[key])
	})

	t.Run("put empty value", func(t *testing.T) {
		key := "test-empty"
		value := []byte{}
		err := client.Put(ctx, key, value, 0)
		require.NoError(t, err)

		// Empty slice and nil are both acceptable for empty values
		assert.Empty(t, server.cacheService.data[key])
	})

	t.Run("put overwrites existing", func(t *testing.T) {
		key := "test-overwrite"
		value1 := []byte("first value")
		value2 := []byte("second value")

		err := client.Put(ctx, key, value1, 0)
		require.NoError(t, err)
		assert.Equal(t, value1, server.cacheService.data[key])

		err = client.Put(ctx, key, value2, 0)
		require.NoError(t, err)
		assert.Equal(t, value2, server.cacheService.data[key])
	})

	t.Run("put large value", func(t *testing.T) {
		key := "test-large"
		value := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
		err := client.Put(ctx, key, value, 0)
		require.NoError(t, err)

		assert.Equal(t, value, server.cacheService.data[key])
	})
}

// TestGet_BasicOperations tests basic Get functionality
func TestGet_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("get existing key", func(t *testing.T) {
		key := "test-key"
		value := []byte("test value")
		server.cacheService.data[key] = value

		data, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, data)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		_, err := client.Get(ctx, "non-existent")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("get large value", func(t *testing.T) {
		key := "large-key"
		value := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
		server.cacheService.data[key] = value

		data, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, data)
	})
}

// TestDelete_BasicOperations tests basic Delete functionality
func TestDelete_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("delete existing key", func(t *testing.T) {
		key := "test-delete"
		server.cacheService.data[key] = []byte("value")

		err := client.Delete(ctx, key)
		require.NoError(t, err)

		// Verify it was deleted
		_, exists := server.cacheService.data[key]
		assert.False(t, exists)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := client.Delete(ctx, "non-existent")
		// Mock server returns NotFound for non-existent keys
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("delete with metadata", func(t *testing.T) {
		key := "test-delete-meta"
		server.cacheService.data[key] = []byte("value")
		server.cacheService.ttls[key] = 3600
		server.cacheService.metadata[key] = map[string]string{"key": "value"}

		err := client.Delete(ctx, key)
		require.NoError(t, err)

		// Verify all associated data was deleted
		_, dataExists := server.cacheService.data[key]
		_, ttlExists := server.cacheService.ttls[key]
		_, metaExists := server.cacheService.metadata[key]
		assert.False(t, dataExists)
		assert.False(t, ttlExists)
		assert.False(t, metaExists)
	})
}

// TestList_BasicOperations tests basic List functionality
func TestList_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Setup test data
	server.cacheService.data["user-1"] = []byte("value1")
	server.cacheService.data["user-2"] = []byte("value2")
	server.cacheService.data["user-3"] = []byte("value3")
	server.cacheService.data["session-1"] = []byte("session1")
	server.cacheService.data["session-2"] = []byte("session2")

	t.Run("list all keys", func(t *testing.T) {
		keys, err := client.List(ctx, "")
		require.NoError(t, err)
		assert.Len(t, keys, 5)
		assert.Contains(t, keys, "user-1")
		assert.Contains(t, keys, "session-2")
	})

	t.Run("list with prefix", func(t *testing.T) {
		keys, err := client.List(ctx, "user-")
		require.NoError(t, err)
		assert.Len(t, keys, 3)
		assert.Contains(t, keys, "user-1")
		assert.Contains(t, keys, "user-2")
		assert.Contains(t, keys, "user-3")
		assert.NotContains(t, keys, "session-1")
	})

	t.Run("list with non-matching prefix", func(t *testing.T) {
		keys, err := client.List(ctx, "non-existent-")
		require.NoError(t, err)
		assert.Empty(t, keys)
	})

	t.Run("list returns sorted keys", func(t *testing.T) {
		keys, err := client.List(ctx, "user-")
		require.NoError(t, err)
		require.Len(t, keys, 3)

		// Verify sorted order
		for i := 1; i < len(keys); i++ {
			assert.LessOrEqual(t, keys[i-1], keys[i])
		}
	})
}

// TestListPage_BasicOperations tests ListPage pagination functionality
func TestListPage_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Setup test data - 15 keys
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("item-%02d", i)
		server.cacheService.data[key] = []byte(fmt.Sprintf("value-%d", i))
	}

	t.Run("first page", func(t *testing.T) {
		keys, token, hasMore, err := client.ListPage(ctx, "", 5, "")
		require.NoError(t, err)
		assert.Len(t, keys, 5)
		assert.True(t, hasMore)
		assert.NotEmpty(t, token)

		// Verify sorted
		for i := 1; i < len(keys); i++ {
			assert.LessOrEqual(t, keys[i-1], keys[i])
		}
	})

	t.Run("second page", func(t *testing.T) {
		// Get first page
		_, token, hasMore, err := client.ListPage(ctx, "", 5, "")
		require.NoError(t, err)
		require.True(t, hasMore)

		// Get second page
		keys, token2, hasMore2, err := client.ListPage(ctx, "", 5, token)
		require.NoError(t, err)
		assert.Len(t, keys, 5)
		assert.True(t, hasMore2)
		assert.NotEmpty(t, token2)
	})

	t.Run("empty results", func(t *testing.T) {
		keys, token, hasMore, err := client.ListPage(ctx, "non-existent-", 10, "")
		require.NoError(t, err)
		assert.Empty(t, keys)
		assert.Empty(t, token)
		assert.False(t, hasMore)
	})
}

// TestListPageWithValues_BasicOperations tests ListPageWithValues pagination with values
func TestListPageWithValues_BasicOperations(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Setup test data - 15 keys with values
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("item-%02d", i)
		server.cacheService.data[key] = []byte(fmt.Sprintf("value-%d", i))
	}

	t.Run("first page with values", func(t *testing.T) {
		entries, token, hasMore, err := client.ListPageWithValues(ctx, "", 5, "")
		require.NoError(t, err)
		assert.Len(t, entries, 5)
		assert.True(t, hasMore)
		assert.NotEmpty(t, token)

		// Verify sorted keys
		for i := 1; i < len(entries); i++ {
			assert.LessOrEqual(t, entries[i-1].Key, entries[i].Key)
		}

		// Verify values match
		for _, e := range entries {
			expected := server.cacheService.data[e.Key]
			assert.Equal(t, expected, e.Value, "value mismatch for key %s", e.Key)
		}
	})

	t.Run("second page with values", func(t *testing.T) {
		_, token, hasMore, err := client.ListPageWithValues(ctx, "", 5, "")
		require.NoError(t, err)
		require.True(t, hasMore)

		entries, token2, hasMore2, err := client.ListPageWithValues(ctx, "", 5, token)
		require.NoError(t, err)
		assert.Len(t, entries, 5)
		assert.True(t, hasMore2)
		assert.NotEmpty(t, token2)

		// Verify values match
		for _, e := range entries {
			expected := server.cacheService.data[e.Key]
			assert.Equal(t, expected, e.Value)
		}
	})

	t.Run("full iteration collects all entries", func(t *testing.T) {
		var all []KeyValue
		token := ""
		for {
			entries, nextToken, hasMore, err := client.ListPageWithValues(ctx, "", 5, token)
			require.NoError(t, err)
			all = append(all, entries...)
			if !hasMore {
				break
			}
			token = nextToken
		}
		assert.Len(t, all, 15)

		// Verify all values
		for _, e := range all {
			expected := server.cacheService.data[e.Key]
			assert.Equal(t, expected, e.Value)
		}
	})

	t.Run("empty results", func(t *testing.T) {
		entries, token, hasMore, err := client.ListPageWithValues(ctx, "non-existent-", 10, "")
		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.Empty(t, token)
		assert.False(t, hasMore)
	})
}

// TestGetRange_BasicOperations tests basic GetRange functionality
func TestGetRange_BasicOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create test data
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	testKey := "range-test-key"
	server.cacheService.data[testKey] = testData

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		key     string
		start   int64
		end     int64
		want    []byte
		wantErr bool
		errCode codes.Code
	}{
		{
			name:  "valid range middle",
			key:   testKey,
			start: 10,
			end:   19, // inclusive: bytes 10-19
			want:  testData[10:20],
		},
		{
			name:  "full range to EOF",
			key:   testKey,
			start: 0,
			end:   0, // end <= 0 means read to EOF
			want:  testData,
		},
		{
			name:  "start only",
			key:   testKey,
			start: 10,
			end:   0, // end <= 0 means read to EOF
			want:  testData[10:],
		},
		{
			name:  "end only",
			key:   testKey,
			start: 0,
			end:   19, // inclusive: bytes 0-19
			want:  testData[0:20],
		},
		{
			name:  "single byte",
			key:   testKey,
			start: 5,
			end:   5, // inclusive: byte 5 only
			want:  testData[5:6],
		},
		{
			name:  "last byte",
			key:   testKey,
			start: int64(len(testData) - 1),
			end:   0, // end <= 0 means read to EOF
			want:  testData[len(testData)-1:],
		},
		{
			name:    "invalid range start > end",
			key:     testKey,
			start:   20,
			end:     10,
			wantErr: true,
			errCode: codes.InvalidArgument,
		},
		{
			name:    "start beyond data length",
			key:     testKey,
			start:   1000,
			end:     0, // end <= 0 means read to EOF
			wantErr: true,
			errCode: codes.InvalidArgument,
		},
		{
			name:    "non-existent key",
			key:     "non-existent",
			start:   0,
			end:     9, // inclusive: bytes 0-9
			wantErr: true,
			errCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := client.GetRange(ctx, tt.key, tt.start, tt.end)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != 0 {
					assert.Equal(t, tt.errCode, status.Code(err))
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, data)
			}
		})
	}
}

// TestGetRangeStream_BasicOperations tests basic GetRangeStream functionality
func TestGetRangeStream_BasicOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create test data
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	testKey := "range-stream-test-key"
	server.cacheService.data[testKey] = testData

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		key     string
		start   int64
		end     int64
		want    []byte
		wantErr bool
		errCode codes.Code
	}{
		{
			name:  "valid range middle",
			key:   testKey,
			start: 10,
			end:   19, // inclusive: bytes 10-19
			want:  testData[10:20],
		},
		{
			name:  "full range to EOF",
			key:   testKey,
			start: 0,
			end:   0, // end <= 0 means read to EOF
			want:  testData,
		},
		{
			name:  "start only",
			key:   testKey,
			start: 10,
			end:   0, // end <= 0 means read to EOF
			want:  testData[10:],
		},
		{
			name:  "end only",
			key:   testKey,
			start: 0,
			end:   19, // inclusive: bytes 0-19
			want:  testData[0:20],
		},
		{
			name:    "invalid range",
			key:     testKey,
			start:   20,
			end:     10,
			wantErr: true,
			errCode: codes.InvalidArgument,
		},
		{
			name:    "non-existent key",
			key:     "non-existent-stream",
			start:   0,
			end:     9, // inclusive: bytes 0-9
			wantErr: true,
			errCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := client.GetRangeStream(ctx, tt.key, tt.start, tt.end, &buf)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != 0 {
					assert.Equal(t, tt.errCode, status.Code(err))
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, buf.Bytes())
			}
		})
	}
}

// TestConcurrent_MixedOperations tests concurrent CRUD operations
func TestConcurrent_MixedOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Counters for operations
	puts := int32(0)
	gets := int32(0)
	deletes := int32(0)
	lists := int32(0)
	errors := int32(0)

	// Concurrent Put operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("put-key-%d-%d", id, j)
				err := client.Put(ctx, key, []byte("value"), 0)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&puts, 1)
				}
			}
		}(i)
	}

	// Concurrent Get operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("put-key-%d-%d", id, j)
				// Small delay to allow puts to complete
				time.Sleep(10 * time.Millisecond)
				_, err := client.Get(ctx, key)
				if err != nil {
					// Some gets may fail if key doesn't exist yet
					continue
				}
				atomic.AddInt32(&gets, 1)
			}
		}(i)
	}

	// Concurrent Delete operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("delete-key-%d-%d", id, j)
				// First put, then delete
				err := client.Put(ctx, key, []byte("temp"), 0)
				if err != nil {
					atomic.AddInt32(&errors, 1)
					continue
				}
				err = client.Delete(ctx, key)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&deletes, 1)
				}
			}
		}(i)
	}

	// Concurrent List operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := client.List(ctx, "put-key-")
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&lists, 1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	// Wait for all operations
	wg.Wait()

	// Verify operations completed
	t.Logf("Operations - Puts: %d, Gets: %d, Deletes: %d, Lists: %d, Errors: %d",
		puts, gets, deletes, lists, errors)

	assert.Equal(t, int32(100), puts, "All puts should complete")
	assert.Greater(t, gets, int32(50), "Most gets should succeed")
	assert.Equal(t, int32(50), deletes, "All deletes should complete")
	assert.Equal(t, int32(50), lists, "All lists should complete")
	assert.Less(t, errors, int32(10), "Errors should be minimal")
}

// TestConcurrent_StreamingOperations tests concurrent streaming
func TestConcurrent_StreamingOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Prepare test data
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := int32(0)
	successes := int32(0)

	// Store large data for streaming
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("stream-key-%d", i)
		server.cacheService.data[key] = largeData
	}

	// Concurrent streaming reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("stream-key-%d", j)
				buf := &testutil.SafeBuffer{}
				err := client.GetStream(ctx, key, buf)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					if buf.Len() == len(largeData) {
						atomic.AddInt32(&successes, 1)
					}
				}
			}
		}(i)
	}

	// Concurrent streaming writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("write-stream-%d", id)
			reader := &testutil.SafeReader{Data: largeData}
			err := client.PutStream(ctx, key, reader, 0)
			if err != nil {
				atomic.AddInt32(&errors, 1)
			} else {
				atomic.AddInt32(&successes, 1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Streaming operations - Successes: %d, Errors: %d", successes, errors)
	assert.Greater(t, successes, int32(50), "Most streaming operations should succeed")
	assert.Less(t, errors, int32(10), "Errors should be minimal")
}

// TestGetRange_LargeData tests range operations with large data
func TestGetRange_LargeData(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create large test data (10MB)
	largeData := make([]byte, 10*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	testKey := "large-data-key"
	server.cacheService.data[testKey] = largeData

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20 * 1024 * 1024)), // 20MB
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("get first MB", func(t *testing.T) {
		data, err := client.GetRange(ctx, testKey, 0, 1024*1024-1) // inclusive: bytes 0 to 1MB-1
		require.NoError(t, err)
		assert.Len(t, data, 1024*1024)
		assert.Equal(t, largeData[0:1024*1024], data)
	})

	t.Run("get middle 2MB", func(t *testing.T) {
		start := int64(4 * 1024 * 1024)
		end := int64(6*1024*1024 - 1) // inclusive: bytes 4MB to 6MB-1
		data, err := client.GetRange(ctx, testKey, start, end)
		require.NoError(t, err)
		assert.Len(t, data, 2*1024*1024)
		assert.Equal(t, largeData[start:end+1], data)
	})

	t.Run("stream last MB", func(t *testing.T) {
		start := int64(9 * 1024 * 1024)
		var buf bytes.Buffer
		err := client.GetRangeStream(ctx, testKey, start, 0, &buf) // end <= 0 means read to EOF
		require.NoError(t, err)
		assert.Len(t, buf.Bytes(), 1024*1024)
		assert.Equal(t, largeData[start:], buf.Bytes())
	})
}
