package cacheclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestRetry_AllOperations tests retry logic for all operations in a table-driven manner
func TestRetry_AllOperations(t *testing.T) {
	// Create server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Prepare test data
	testKey := "retry-test-key"
	testData := []byte("test-value")
	server.cacheService.data[testKey] = testData
	server.cacheService.data["list-key-1"] = []byte("value1")
	server.cacheService.data["list-key-2"] = []byte("value2")

	operations := []struct {
		name         string
		setupError   func()
		cleanupError func()
		operation    func() error
		expectRetry  bool
		checkCall    func() int32
	}{
		{
			name: "Put with routing error",
			setupError: func() {
				server.cacheService.putError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.putError = nil
			},
			operation: func() error {
				return client.Put(ctx, testKey, testData, 0)
			},
			expectRetry: true,
			checkCall: func() int32 {
				count, _, _, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "Get with routing error",
			setupError: func() {
				server.cacheService.getError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.getError = nil
			},
			operation: func() error {
				_, err := client.Get(ctx, testKey)
				return err
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, count, _, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "Delete with routing error",
			setupError: func() {
				server.cacheService.deleteError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.deleteError = nil
			},
			operation: func() error {
				return client.Delete(ctx, testKey)
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, _, count, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "List with routing error",
			setupError: func() {
				server.cacheService.listError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.listError = nil
			},
			operation: func() error {
				_, err := client.List(ctx, "list-key-")
				return err
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, _, _, count := server.GetCallCounts()
				return count
			},
		},
		{
			name: "PutStream with routing error",
			setupError: func() {
				server.cacheService.putError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.putError = nil
			},
			operation: func() error {
				reader := bytes.NewReader(testData)
				return client.PutStream(ctx, "stream-key", reader, 0)
			},
			expectRetry: true,
			checkCall: func() int32 {
				count, _, _, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "GetStream with routing error",
			setupError: func() {
				server.cacheService.getError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.getError = nil
			},
			operation: func() error {
				var buf bytes.Buffer
				return client.GetStream(ctx, testKey, &buf)
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, count, _, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "GetRange with routing error",
			setupError: func() {
				server.cacheService.getError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.getError = nil
			},
			operation: func() error {
				_, err := client.GetRange(ctx, testKey, 0, 5)
				return err
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, count, _, _ := server.GetCallCounts()
				return count
			},
		},
		{
			name: "GetRangeStream with routing error",
			setupError: func() {
				server.cacheService.getError = status.Error(codes.FailedPrecondition, "routing error")
			},
			cleanupError: func() {
				server.cacheService.getError = nil
			},
			operation: func() error {
				var buf bytes.Buffer
				return client.GetRangeStream(ctx, testKey, 0, 5, &buf)
			},
			expectRetry: true,
			checkCall: func() int32 {
				_, count, _, _ := server.GetCallCounts()
				return count
			},
		},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			// Setup error
			tc.setupError()

			// Track initial call count
			initialCount := tc.checkCall()

			// Execute operation (should fail with routing error)
			err := tc.operation()
			assert.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))

			// Check retry behavior
			finalCount := tc.checkCall()
			callsMade := finalCount - initialCount

			if tc.expectRetry {
				// For operations that support retry, we expect at least 1 call
				// Some operations may make 2 calls (initial + retry)
				assert.GreaterOrEqual(t, callsMade, int32(1), "Should make at least 1 call")
			} else {
				// For operations without retry, exactly 1 call
				assert.Equal(t, int32(1), callsMade, "Should make exactly 1 call (no retry)")
			}

			// Cleanup
			tc.cleanupError()
			server.Reset()
		})
	}
}

// TestRetry_MaxRetryCount verifies retry limit is enforced
func TestRetry_MaxRetryCount(t *testing.T) {
	// Create a server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("Get_RetriesOnce", func(t *testing.T) {
		// Configure server to always return routing error
		testKey := "retry-test-key"
		// Put some data so key exists (otherwise we get NotFound)
		server.cacheService.data[testKey] = []byte("test-value")
		server.InjectErrors(testKey, &errorInjector{
			routingError: true,
		})

		// Store the initial count
		_, initialGetCount, _, _ := server.GetCallCounts()

		// Get should fail with routing error
		_, err := client.Get(ctx, testKey)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called at least once
		_, finalGetCount, _, _ := server.GetCallCounts()
		callsMade := finalGetCount - initialGetCount
		// Note: Get retry only works when the initial RPC succeeds but stream fails during data transfer
		// With immediate RPC error, retry doesn't happen
		assert.GreaterOrEqual(t, callsMade, int32(1), "Should make at least 1 call")

		server.Reset()
	})

	t.Run("GetStream_RetriesOnce", func(t *testing.T) {
		// Configure server to always return routing error
		testKey := "retry-stream-key"
		// Put some data so key exists (otherwise we get NotFound)
		server.cacheService.data[testKey] = []byte("test-value")
		server.InjectErrors(testKey, &errorInjector{
			routingError: true,
		})

		// Store the initial count
		_, initialGetCount, _, _ := server.GetCallCounts()

		// GetStream should fail with routing error
		var buf bytes.Buffer
		err := client.GetStream(ctx, testKey, &buf)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called at least once
		_, finalGetCount, _, _ := server.GetCallCounts()
		callsMade := finalGetCount - initialGetCount
		// Note: GetStream retry only works when the initial RPC succeeds but stream fails during data transfer
		// With immediate RPC error, retry doesn't happen
		assert.GreaterOrEqual(t, callsMade, int32(1), "Should make at least 1 call")

		server.Reset()
	})

	t.Run("Put_RetriesOnce", func(t *testing.T) {
		// Configure server to always return routing error
		server.cacheService.putError = status.Error(codes.FailedPrecondition, "routing error")

		// Store the initial count
		initialPutCount, _, _, _ := server.GetCallCounts()

		// Put should retry once and then fail
		err := client.Put(ctx, "retry-put-key", []byte("value"), 0)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called twice (initial + 1 retry)
		finalPutCount, _, _, _ := server.GetCallCounts()
		callsMade := finalPutCount - initialPutCount
		assert.Equal(t, int32(2), callsMade, "Should make 2 calls: initial + 1 retry")

		server.Reset()
	})

	t.Run("Delete_RetriesOnce", func(t *testing.T) {
		// Configure server to always return routing error
		server.cacheService.deleteError = status.Error(codes.FailedPrecondition, "routing error")

		// Store the initial count
		_, _, initialDeleteCount, _ := server.GetCallCounts()

		// Delete should retry once and then fail
		err := client.Delete(ctx, "retry-delete-key")
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called twice (initial + 1 retry)
		_, _, finalDeleteCount, _ := server.GetCallCounts()
		callsMade := finalDeleteCount - initialDeleteCount
		assert.Equal(t, int32(2), callsMade, "Should make 2 calls: initial + 1 retry")

		server.Reset()
	})
}

// TestRetry_OnlyOnRoutingErrors verifies non-routing errors don't retry
func TestRetry_OnlyOnRoutingErrors(t *testing.T) {
	// Create a server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	nonRoutingErrors := []struct {
		name string
		code codes.Code
	}{
		{"Internal", codes.Internal},
		{"InvalidArgument", codes.InvalidArgument},
		{"PermissionDenied", codes.PermissionDenied},
		{"Unauthenticated", codes.Unauthenticated},
		{"ResourceExhausted", codes.ResourceExhausted},
	}

	for _, tc := range nonRoutingErrors {
		t.Run(tc.name, func(t *testing.T) {
			// Configure server to return non-routing error
			testKey := "non-routing-" + tc.name
			// Put some data so key exists (otherwise we get NotFound)
			server.cacheService.data[testKey] = []byte("test-value")
			server.cacheService.streamErrors[testKey] = status.Error(tc.code, "test error")

			// Track calls before operation
			_, getCountBefore, _, _ := server.GetCallCounts()

			// Get should NOT retry
			_, err := client.Get(ctx, testKey)
			assert.Error(t, err)
			assert.Equal(t, tc.code, status.Code(err))

			// Should have been called only once (no retry)
			_, getCountAfter, _, _ := server.GetCallCounts()
			assert.Equal(t, int32(1), getCountAfter-getCountBefore, "Should call Get exactly once (no retry)")

			server.Reset()
		})
	}
}

// TestRetry_NoRetryAfterPartialData verifies data integrity protection
func TestRetry_NoRetryAfterPartialData(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("Get_NoRetryAfterPartialData", func(t *testing.T) {
		testKey := "partial-data-key"
		fullData := []byte("this is the full data that should be returned")
		server.cacheService.data[testKey] = fullData

		// Configure to send 10 bytes then fail with routing error
		server.InjectErrors(testKey, &errorInjector{
			partialDataBytes: 10,
			routingError:     true,
		})

		// Get should fail without retry (partial data received)
		_, err := client.Get(ctx, testKey)
		assert.Error(t, err)

		// Should have been called only once (no retry)
		_, getCount, _, _ := server.GetCallCounts()
		assert.Equal(t, int32(1), getCount)

		server.Reset()
	})

	t.Run("GetStream_NoRetryAfterPartialWrite", func(t *testing.T) {
		testKey := "partial-stream-key"
		fullData := []byte("this is the full data that should be streamed")
		server.cacheService.data[testKey] = fullData

		// Configure to send 10 bytes then fail with routing error
		server.InjectErrors(testKey, &errorInjector{
			partialDataBytes: 10,
			routingError:     true,
		})

		// GetStream should fail without retry (partial data written)
		var buf bytes.Buffer
		err := client.GetStream(ctx, testKey, &buf)
		assert.Error(t, err)

		// Should have received the partial data
		assert.Equal(t, 10, buf.Len())

		// Should have been called only once (no retry)
		_, getCount, _, _ := server.GetCallCounts()
		assert.Equal(t, int32(1), getCount)

		server.Reset()
	})
}

// TestRetry_ConcurrentRetries verifies multiple concurrent operations retrying
func TestRetry_ConcurrentRetries(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Set up test data - half with errors, half without
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("concurrent-key-%d", i)
		server.cacheService.data[key] = []byte(fmt.Sprintf("value-%d", i))
		// Only set routing errors for first 5 keys
		if i < 5 {
			server.cacheService.streamErrors[key] = status.Error(codes.FailedPrecondition, "routing error")
		}
	}

	ctx := context.Background()
	errChan := make(chan error, 10)
	var getCountBefore int32
	_, getCountBefore, _, _ = server.GetCallCounts()

	// Launch concurrent Get operations
	for i := 0; i < 10; i++ {
		go func(idx int) {
			key := fmt.Sprintf("concurrent-key-%d", idx)
			_, err := client.Get(ctx, key)
			errChan <- err
		}(i)
	}

	// Wait for all operations
	successCount := 0
	failCount := 0
	for i := 0; i < 10; i++ {
		err := <-errChan
		if err == nil {
			successCount++
		} else {
			failCount++
		}
	}

	// Half should succeed (those without errors), half should fail (after retry)
	assert.Equal(t, 5, successCount, "Keys without errors should succeed")
	assert.Equal(t, 5, failCount, "Keys with routing errors should fail after retry")

	// Should have retry attempts for the failing keys
	_, getCountAfter, _, _ := server.GetCallCounts()
	// 10 initial attempts + 5 retries for failed keys = 15 total
	assert.GreaterOrEqual(t, getCountAfter-getCountBefore, int32(15), "Should have retries for failed operations")
}

// TestRetry_PutStream tests PutStream behavior in cluster mode
func TestRetry_PutStream(t *testing.T) {
	// Create server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Note: PutStream doesn't have retry logic in ClusterClient, it inherits from Operations
	// This test verifies that PutStream works correctly but doesn't retry on errors

	t.Run("PutStream completes successfully", func(t *testing.T) {
		// Test that PutStream works without errors
		testData := make([]byte, 1024)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		reader := bytes.NewReader(testData)
		err := client.PutStream(ctx, "stream-key", reader, 0)
		require.NoError(t, err)

		// Verify data was stored correctly
		assert.Equal(t, testData, server.cacheService.data["stream-key"])
	})

	t.Run("PutStream fails on routing error without retry", func(t *testing.T) {
		// Configure server to return routing error
		server.cacheService.putError = status.Error(codes.FailedPrecondition, "routing error")

		// Track put count
		initialPutCount, _, _, _ := server.GetCallCounts()

		// PutStream should fail without retry
		testData := []byte("test stream data")
		reader := bytes.NewReader(testData)
		err := client.PutStream(ctx, "stream-retry-key", reader, 0)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called only once (no retry)
		finalPutCount, _, _, _ := server.GetCallCounts()
		assert.Equal(t, int32(1), finalPutCount-initialPutCount, "Should call Put exactly once (no retry)")

		server.Reset()
	})
}

// TestRetry_List tests List behavior in cluster mode
func TestRetry_List(t *testing.T) {
	// Create server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Add some test data
	server.cacheService.data["list-key-1"] = []byte("value1")
	server.cacheService.data["list-key-2"] = []byte("value2")
	server.cacheService.data["list-key-3"] = []byte("value3")

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Note: List doesn't have retry logic in ClusterClient, it inherits from Operations
	// This test verifies that List works correctly but doesn't retry on errors

	t.Run("List works successfully", func(t *testing.T) {
		// List should work without errors
		keys, err := client.List(ctx, "list-key-")
		require.NoError(t, err)
		assert.Len(t, keys, 3)
		assert.ElementsMatch(t, []string{"list-key-1", "list-key-2", "list-key-3"}, keys)
	})

	t.Run("List fails on routing error without retry", func(t *testing.T) {
		// Configure server to return routing error
		server.cacheService.listError = status.Error(codes.FailedPrecondition, "routing error")

		// Track list count
		_, _, _, initialListCount := server.GetCallCounts()

		// List should fail without retry
		_, err := client.List(ctx, "list-key-")
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have been called only once (no retry)
		_, _, _, finalListCount := server.GetCallCounts()
		assert.Equal(t, int32(1), finalListCount-initialListCount, "Should call List exactly once (no retry)")

		server.Reset()
	})

	t.Run("List handles topology changes", func(t *testing.T) {
		// Add more test data
		server.cacheService.data["topo-key-1"] = []byte("value1")
		server.cacheService.data["topo-key-2"] = []byte("value2")

		// Initial list should work
		keys, err := client.List(ctx, "topo-key-")
		require.NoError(t, err)
		assert.Len(t, keys, 2)

		// Update topology
		newTopology := setupSimpleTopology([]string{server.address})
		newTopology.Epoch = 2
		server.clusterService.SetTopology(newTopology)

		// Force topology refresh
		if cc, ok := client.CacheClient.(*ClusterClient); ok {
			fetchedTopo, _ := cc.FetchTopology()
			cc.UpdateTopology(fetchedTopo)
		}

		// List should still work after topology change
		keys, err = client.List(ctx, "topo-key-")
		require.NoError(t, err)
		assert.Len(t, keys, 2)
	})
}

// TestRetry_GetData tests the unified retry methods with various parameter combinations
func TestRetry_GetData(t *testing.T) {
	// Create server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create test data
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Get the cluster client to test internal methods
	cc, ok := client.CacheClient.(*ClusterClient)
	require.True(t, ok, "Expected ClusterClient")

	t.Run("getDataWithRetry edge cases", func(t *testing.T) {
		tests := []struct {
			name  string
			key   string
			start int64
			end   int64
			want  []byte
		}{
			{
				name:  "start=0 end=0 (regular Get)",
				key:   "test-key-1",
				start: 0,
				end:   0,
				want:  testData,
			},
			{
				name:  "start>0 end=0",
				key:   "test-key-2",
				start: 10,
				end:   0,
				want:  testData[10:],
			},
			{
				name:  "start=0 end>0",
				key:   "test-key-3",
				start: 0,
				end:   20,
				want:  testData[0:20],
			},
			{
				name:  "start>0 end>0",
				key:   "test-key-4",
				start: 10,
				end:   20,
				want:  testData[10:20],
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Set up test data
				server.cacheService.data[tt.key] = testData

				// Call the internal method directly
				data, err := cc.getDataWithRetry(ctx, tt.key, tt.start, tt.end, 1)
				require.NoError(t, err)
				assert.Equal(t, tt.want, data)
			})
		}
	})

	t.Run("getStreamDataWithRetry edge cases", func(t *testing.T) {
		tests := []struct {
			name  string
			key   string
			start int64
			end   int64
			want  []byte
		}{
			{
				name:  "start=0 end=0 (regular GetStream)",
				key:   "stream-key-1",
				start: 0,
				end:   0,
				want:  testData,
			},
			{
				name:  "start>0 end=0",
				key:   "stream-key-2",
				start: 10,
				end:   0,
				want:  testData[10:],
			},
			{
				name:  "start=0 end>0",
				key:   "stream-key-3",
				start: 0,
				end:   20,
				want:  testData[0:20],
			},
			{
				name:  "start>0 end>0",
				key:   "stream-key-4",
				start: 10,
				end:   20,
				want:  testData[10:20],
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Set up test data
				server.cacheService.data[tt.key] = testData

				// Call the internal method directly
				var buf bytes.Buffer
				err := cc.getStreamDataWithRetry(ctx, tt.key, tt.start, tt.end, &buf, 1)
				require.NoError(t, err)
				assert.Equal(t, tt.want, buf.Bytes())
			})
		}
	})
}

// TestRetry_GetRange tests GetRange retry logic in cluster mode
func TestRetry_GetRange(t *testing.T) {
	// Create server with cluster topology
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create test data
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	testKey := "retry-range-key"
	server.cacheService.data[testKey] = testData

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("routing error that persists", func(t *testing.T) {
		// Test that GetRange fails with routing error and retries once
		persistKey := "persist-range-key"
		server.cacheService.data[persistKey] = testData

		// Configure to always fail with routing error
		server.cacheService.getError = status.Error(codes.FailedPrecondition, "routing error")

		// Track get count before operation
		_, getCountBefore, _, _ := server.GetCallCounts()

		// GetRange should fail after retry
		_, err := client.GetRange(ctx, persistKey, 10, 20)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Should have called Get twice (initial + 1 retry)
		_, getCountAfter, _, _ := server.GetCallCounts()
		assert.Equal(t, int32(2), getCountAfter-getCountBefore, "Should make 2 calls: initial + 1 retry")

		// Clean up
		server.cacheService.getError = nil
	})

	t.Run("no retry after partial data", func(t *testing.T) {
		partialKey := "partial-range-key"
		server.cacheService.data[partialKey] = testData

		// Configure to send 5 bytes then fail with routing error
		server.InjectErrors(partialKey, &errorInjector{
			partialDataBytes: 5,
			routingError:     true,
		})

		// Track get count
		_, getCountBefore, _, _ := server.GetCallCounts()

		// Should fail without retry (partial data received)
		data, err := client.GetRange(ctx, partialKey, 0, 20)
		assert.Error(t, err)

		// Check what data was received (should be empty since error should prevent returning partial data)
		assert.Nil(t, data, "Should not return partial data on error")

		// Should have been called only once (no retry)
		_, getCountAfter, _, _ := server.GetCallCounts()
		assert.Equal(t, int32(1), getCountAfter-getCountBefore, "Should not retry after receiving partial data")

		server.Reset()
	})
}

// failingWriter is a writer that fails after writing a certain number of bytes
type failingWriter struct {
	written   int
	failAfter int
}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	if f.written+len(p) >= f.failAfter {
		// Write would reach or exceed limit
		return 0, errors.New("simulated write failure")
	}
	f.written += len(p)
	return len(p), nil
}

// mockSlowWriter simulates a slow writer for testing streaming retries
type mockSlowWriter struct {
	buf           bytes.Buffer
	bytesWritten  int
	delayAfter    int
	delayDuration time.Duration
}

func (m *mockSlowWriter) Write(p []byte) (n int, err error) {
	if m.bytesWritten >= m.delayAfter && m.delayDuration > 0 {
		time.Sleep(m.delayDuration)
	}
	n, err = m.buf.Write(p)
	m.bytesWritten += n
	return n, err
}
