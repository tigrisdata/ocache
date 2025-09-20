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
			end:   20,
			want:  testData[10:20],
		},
		{
			name:  "full range with zeros",
			key:   testKey,
			start: 0,
			end:   0,
			want:  testData,
		},
		{
			name:  "start only",
			key:   testKey,
			start: 10,
			end:   0,
			want:  testData[10:],
		},
		{
			name:  "end only",
			key:   testKey,
			start: 0,
			end:   20,
			want:  testData[0:20],
		},
		{
			name:  "single byte",
			key:   testKey,
			start: 5,
			end:   6,
			want:  testData[5:6],
		},
		{
			name:  "last byte",
			key:   testKey,
			start: int64(len(testData) - 1),
			end:   0,
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
			end:     0,
			wantErr: true,
			errCode: codes.InvalidArgument,
		},
		{
			name:    "non-existent key",
			key:     "non-existent",
			start:   0,
			end:     10,
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
			end:   20,
			want:  testData[10:20],
		},
		{
			name:  "full range with zeros",
			key:   testKey,
			start: 0,
			end:   0,
			want:  testData,
		},
		{
			name:  "start only",
			key:   testKey,
			start: 10,
			end:   0,
			want:  testData[10:],
		},
		{
			name:  "end only",
			key:   testKey,
			start: 0,
			end:   20,
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
			end:     10,
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
		data, err := client.GetRange(ctx, testKey, 0, 1024*1024)
		require.NoError(t, err)
		assert.Len(t, data, 1024*1024)
		assert.Equal(t, largeData[0:1024*1024], data)
	})

	t.Run("get middle 2MB", func(t *testing.T) {
		start := int64(4 * 1024 * 1024)
		end := int64(6 * 1024 * 1024)
		data, err := client.GetRange(ctx, testKey, start, end)
		require.NoError(t, err)
		assert.Len(t, data, 2*1024*1024)
		assert.Equal(t, largeData[start:end], data)
	})

	t.Run("stream last MB", func(t *testing.T) {
		start := int64(9 * 1024 * 1024)
		var buf bytes.Buffer
		err := client.GetRangeStream(ctx, testKey, start, 0, &buf)
		require.NoError(t, err)
		assert.Len(t, buf.Bytes(), 1024*1024)
		assert.Equal(t, largeData[start:], buf.Bytes())
	})
}
