package coordinator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// mockRouterClusterService implements a minimal ClusterServiceServer for router testing
type mockRouterClusterService struct {
	clusterpb.UnimplementedClusterServiceServer
	failUntil time.Time
	nodeID    string
}

func (m *mockRouterClusterService) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	if time.Now().Before(m.failUntil) {
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}
	return &clusterpb.HeartbeatResponse{
		Epoch: 1,
	}, nil
}

type mockRouterCacheService struct {
	pb.UnimplementedCacheServiceServer
}

func (m *mockRouterCacheService) Put(stream pb.CacheService_PutServer) error {
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

func (m *mockRouterCacheService) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	return &pb.PutResponse{Success: true}, nil
}

func (m *mockRouterCacheService) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	return stream.Send(&pb.GetResponse{Data: []byte("hello world")})
}

func (m *mockRouterCacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{Success: true}, nil
}

func (m *mockRouterCacheService) List(req *pb.ListRequest, stream pb.CacheService_ListServer) error {
	return stream.Send(&pb.ListResponse{Keys: []string{"key1", "key2"}})
}

func startMockRouterServer(t *testing.T, nodeID string) (string, string, *grpc.Server, *grpc.Server, *mockRouterClusterService, *mockRouterCacheService) {
	clusterLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	cacheLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcClusterServer := grpc.NewServer()
	mockClusterService := &mockRouterClusterService{nodeID: nodeID}
	clusterpb.RegisterClusterServiceServer(grpcClusterServer, mockClusterService)

	grpcCacheServer := grpc.NewServer()
	mockCacheService := &mockRouterCacheService{}
	pb.RegisterCacheServiceServer(grpcCacheServer, mockCacheService)

	go func() {
		if err := grpcClusterServer.Serve(clusterLis); err != nil {
			t.Logf("Mock server stopped: %v", err)
		}
	}()

	go func() {
		if err := grpcCacheServer.Serve(cacheLis); err != nil {
			t.Logf("Mock server stopped: %v", err)
		}
	}()

	return cacheLis.Addr().String(), clusterLis.Addr().String(), grpcClusterServer, grpcCacheServer, mockClusterService, mockCacheService
}

// TestRouter_UsesListenAddressForCacheOperations verifies router connects to listen address for cache operations
func TestRouter_UsesListenAddressForCacheOperations(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Add local node
	err = ring.AddNode("local-node", "localhost:7000", "localhost:9000")
	require.NoError(t, err)

	// Start mock servers on different ports
	// Cache service on port that will be used as listen address
	cacheLis, err := net.Listen("tcp", "localhost:9500")
	require.NoError(t, err)
	defer cacheLis.Close()

	// Cluster service on port that will be used as cluster address
	clusterLis, err := net.Listen("tcp", "localhost:7500")
	require.NoError(t, err)
	defer clusterLis.Close()

	// Track which server gets the connection
	var cacheServiceCalled bool
	var clusterServiceCalled bool

	// Start cache server (should receive requests)
	grpcCacheServer := grpc.NewServer()
	// Register tracked service
	pb.RegisterCacheServiceServer(grpcCacheServer, &trackedCacheService{
		mockRouterCacheService: mockRouterCacheService{},
		called:                 &cacheServiceCalled,
	})

	go func() {
		grpcCacheServer.Serve(cacheLis)
	}()
	defer grpcCacheServer.Stop()

	// Start cluster server (should NOT receive cache requests)
	grpcClusterServer := grpc.NewServer()
	// Also register cache service on cluster port to detect wrong routing
	pb.RegisterCacheServiceServer(grpcClusterServer, &trackedCacheService{
		mockRouterCacheService: mockRouterCacheService{},
		called:                 &clusterServiceCalled,
	})

	go func() {
		grpcClusterServer.Serve(clusterLis)
	}()
	defer grpcClusterServer.Stop()

	// Add remote node with DIFFERENT cluster and listen addresses
	err = ring.AddNode("remote-node", "localhost:7500", "localhost:9500")
	require.NoError(t, err)

	// Create router
	router := NewRouter(ring, "local-node")
	defer router.Close()

	// Find a key that routes to remote-node
	var testKey string
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		node, err := ring.GetNode(key)
		require.NoError(t, err)
		if node.ID == "remote-node" {
			testKey = key
			break
		}
	}
	require.NotEmpty(t, testKey, "Should find a key that routes to remote-node")

	// Route to remote node
	client, err := router.Route(testKey)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Make a request
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: testKey})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify the correct server was called
	assert.True(t, cacheServiceCalled, "Cache service on listen address (9500) should be called")
	assert.False(t, clusterServiceCalled, "Cluster service on cluster address (7500) should NOT be called")

	// Verify router cached the correct address
	stats := router.GetConnectionStats()
	assert.Contains(t, stats, "remote-node")

	// Verify the router is using the listen address by checking internal state
	router.mu.RLock()
	if state, exists := router.clients["remote-node"]; exists && state.conn != nil {
		// Get the target from the connection
		target := state.conn.Target()
		assert.Contains(t, target, "9500", "Router should connect to listen address port 9500")
		assert.NotContains(t, target, "7500", "Router should NOT connect to cluster address port 7500")
	}
	router.mu.RUnlock()
}

// Helper type to track service calls
type trackedCacheService struct {
	mockRouterCacheService
	called *bool
}

func (t *trackedCacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	*t.called = true
	return t.mockRouterCacheService.Delete(ctx, req)
}

func (t *trackedCacheService) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	*t.called = true
	return t.mockRouterCacheService.PutObject(ctx, req)
}

func TestRouter_BasicRouting(t *testing.T) {
	// Create ring and add nodes
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Add self to ring first (coordinator does this)
	err = ring.AddNode("local-node", "localhost:7999", "localhost:9999")
	require.NoError(t, err)

	// Start mock remote node
	cacheAddr, clusterAddr, grpcClusterServer, grpcCacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcClusterServer.Stop()
	defer grpcCacheServer.Stop()

	err = ring.AddNode("remote-node", clusterAddr, cacheAddr)
	require.NoError(t, err)

	// Create router with custom config for faster testing
	config := &RouterConfig{
		ConnectionTimeout:       1 * time.Second,
		MaxSendMsgSize:          1024 * 1024,
		MaxRecvMsgSize:          1024 * 1024,
		MaxRetries:              2,
		InitialRetryBackoff:     50 * time.Millisecond,
		MaxRetryBackoff:         500 * time.Millisecond,
		KeepaliveTime:           5 * time.Second,
		KeepaliveTimeout:        2 * time.Second,
		CircuitBreakerThreshold: 3,
		CircuitBreakerTimeout:   2 * time.Second,
	}
	router := NewRouterWithConfig(ring, "local-node", config)
	defer router.Close()

	// Test routing to local node (should return nil)
	// Find a key that maps to local node
	var localKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("key%d", i)
		if ring.IsLocal(testKey) {
			localKey = testKey
			break
		}
	}
	require.NotEmpty(t, localKey, "Could not find key that maps to local node")

	client, err := router.Route(localKey)
	// Should return an error for local keys (defensive check)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, errors.Is(err, ErrLocalRouting), "Expected ErrLocalRouting")
	var routerErr *RouterError
	assert.True(t, errors.As(err, &routerErr), "Expected RouterError type")
	assert.Equal(t, "local-node", routerErr.NodeID)
	assert.Equal(t, localKey, routerErr.Key)

	// Test routing to remote node
	var remoteKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("test%d", i)
		node, _ := ring.GetNode(testKey)
		if node != nil && node.ID == "remote-node" {
			remoteKey = testKey
			break
		}
	}
	require.NotEmpty(t, remoteKey, "Could not find key that maps to remote node")

	client, err = router.Route(remoteKey)
	assert.NoError(t, err)
	assert.NotNil(t, client, "Remote routing should return a client")
}

func TestRouter_ConnectionRetry(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Start mock server that will fail initially
	listenAddr, clusterAddr, grpcServer, _, mockService, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	// Make the service fail for 100ms
	mockService.failUntil = time.Now().Add(100 * time.Millisecond)

	err = ring.AddNode("remote-node", listenAddr, clusterAddr)
	require.NoError(t, err)

	// Create router with retry config
	config := &RouterConfig{
		ConnectionTimeout:       1 * time.Second,
		MaxSendMsgSize:          1024 * 1024,
		MaxRecvMsgSize:          1024 * 1024,
		MaxRetries:              3,
		InitialRetryBackoff:     50 * time.Millisecond,
		MaxRetryBackoff:         500 * time.Millisecond,
		KeepaliveTime:           5 * time.Second,
		KeepaliveTimeout:        2 * time.Second,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   2 * time.Second,
	}
	router := NewRouterWithConfig(ring, "local-node", config)
	defer router.Close()

	// Find a key that routes to remote node
	var remoteKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("key%d", i)
		node, _ := ring.GetNode(testKey)
		if node != nil && node.ID == "remote-node" {
			remoteKey = testKey
			break
		}
	}
	require.NotEmpty(t, remoteKey)

	// Connection should succeed (non-blocking dial)
	client, err := router.Route(remoteKey)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestRouter_CircuitBreaker(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Add a non-existent node
	err = ring.AddNode("dead-node", "localhost:77777", "localhost:99999")
	require.NoError(t, err)

	// Create router with low circuit breaker threshold
	config := &RouterConfig{
		ConnectionTimeout:       100 * time.Millisecond,
		MaxSendMsgSize:          1024 * 1024,
		MaxRecvMsgSize:          1024 * 1024,
		MaxRetries:              0, // No retries for this test
		InitialRetryBackoff:     50 * time.Millisecond,
		MaxRetryBackoff:         500 * time.Millisecond,
		KeepaliveTime:           5 * time.Second,
		KeepaliveTimeout:        2 * time.Second,
		CircuitBreakerThreshold: 2,
		CircuitBreakerTimeout:   1 * time.Second,
	}
	router := NewRouterWithConfig(ring, "local-node", config)
	defer router.Close()

	// Find a key that routes to dead node
	var deadKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("key%d", i)
		node, _ := ring.GetNode(testKey)
		if node != nil && node.ID == "dead-node" {
			deadKey = testKey
			break
		}
	}
	require.NotEmpty(t, deadKey)

	// First attempts should work (creates connection without blocking)
	// The connection will be in a connecting state
	_, err = router.Route(deadKey)
	assert.NoError(t, err)

	// Wait a bit for connection to realize it's failed
	time.Sleep(200 * time.Millisecond)

	// Try multiple times to trigger circuit breaker
	// Note: Circuit breaker tracks failures, not connection attempts
	// Since we're using non-blocking dial, we need to check connection state
	for i := 0; i < config.CircuitBreakerThreshold+1; i++ {
		router.Route(deadKey)
		time.Sleep(50 * time.Millisecond)
	}

	// Circuit should be open now
	_, err = router.Route(deadKey)
	require.Error(t, err, "Expected error when circuit breaker is open")
	// After threshold failures, circuit breaker should be open
	assert.True(t, errors.Is(err, ErrCircuitBreakerOpen) || errors.Is(err, ErrMaxRetriesExceeded),
		"Expected circuit breaker or max retries error")

	// Wait for circuit breaker timeout
	time.Sleep(config.CircuitBreakerTimeout + 100*time.Millisecond)

	// Circuit should be closed now, should be able to try again
	_, err = router.Route(deadKey)
	// Won't get circuit breaker error immediately after reset
	if err != nil {
		assert.False(t, errors.Is(err, ErrCircuitBreakerOpen), "Circuit breaker should be closed after timeout")
	}
}

func TestRouter_RemoveClient(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Start mock remote node
	listenAddr, clusterAddr, grpcServer, _, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	err = ring.AddNode("remote-node", listenAddr, clusterAddr)
	require.NoError(t, err)

	router := NewRouter(ring, "local-node")
	defer router.Close()

	// Find a key for remote node
	var remoteKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("key%d", i)
		node, _ := ring.GetNode(testKey)
		if node != nil && node.ID == "remote-node" {
			remoteKey = testKey
			break
		}
	}
	require.NotEmpty(t, remoteKey)

	// Create connection
	client, err := router.Route(remoteKey)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Remove client
	router.RemoveClient("remote-node")

	// Should create new connection
	client2, err := router.Route(remoteKey)
	assert.NoError(t, err)
	assert.NotNil(t, client2)
}

func TestRouter_RefreshConnections(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Start two mock nodes
	addr1, clusterAddr1, server1, _, _, _ := startMockRouterServer(t, "node1")
	defer server1.Stop()
	addr2, clusterAddr2, server2, _, _, _ := startMockRouterServer(t, "node2")
	defer server2.Stop()

	err = ring.AddNode("node1", addr1, clusterAddr1)
	require.NoError(t, err)
	err = ring.AddNode("node2", addr2, clusterAddr2)
	require.NoError(t, err)

	router := NewRouter(ring, "local-node")
	defer router.Close()

	// Create connections to both nodes
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		router.Route(key)
	}

	// Remove node2 from ring
	err = ring.RemoveNode("node2")
	require.NoError(t, err)

	// Refresh connections
	router.RefreshConnections()

	// Check that only node1 connection remains
	stats := router.GetConnectionStats()
	assert.Contains(t, stats, "node1")
	assert.NotContains(t, stats, "node2")
}

func TestRouter_GetConnectionStats(t *testing.T) {
	// Create ring
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Start mock node
	listenAddr, clusterAddr, grpcServer, _, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	err = ring.AddNode("remote-node", listenAddr, clusterAddr)
	require.NoError(t, err)

	router := NewRouter(ring, "local-node")
	defer router.Close()

	// Initially no stats
	stats := router.GetConnectionStats()
	assert.Empty(t, stats)

	// Create connection
	var remoteKey string
	for i := 0; i < 100; i++ {
		testKey := fmt.Sprintf("key%d", i)
		node, _ := ring.GetNode(testKey)
		if node != nil && node.ID == "remote-node" {
			remoteKey = testKey
			break
		}
	}
	require.NotEmpty(t, remoteKey)

	_, err = router.Route(remoteKey)
	assert.NoError(t, err)

	// Check stats
	stats = router.GetConnectionStats()
	assert.Contains(t, stats, "remote-node")

	nodeStat := stats["remote-node"]
	assert.Equal(t, int32(0), nodeStat.FailureCount)
	assert.False(t, nodeStat.CircuitOpen)
	// State might be IDLE, CONNECTING, or READY depending on timing
	assert.Contains(t, []string{
		connectivity.Idle.String(),
		connectivity.Connecting.String(),
		connectivity.Ready.String(),
	}, nodeStat.State)
}

func TestRouter_TypedErrors(t *testing.T) {
	tests := []struct {
		name         string
		error        error
		expectedType error
		isRetryable  bool
		isTemporary  bool
	}{
		{
			name:         "local routing error",
			error:        NewLocalRoutingError("node1", "key1"),
			expectedType: ErrLocalRouting,
			isRetryable:  false,
			isTemporary:  false,
		},
		{
			name:         "circuit breaker open",
			error:        NewCircuitBreakerOpenError("node2"),
			expectedType: ErrCircuitBreakerOpen,
			isRetryable:  false,
			isTemporary:  false,
		},
		{
			name:         "node not found",
			error:        NewNodeNotFoundError("node3", "key3"),
			expectedType: ErrNodeNotFound,
			isRetryable:  false,
			isTemporary:  false,
		},
		{
			name:         "connection failed",
			error:        NewConnectionFailedError("node4", "localhost:9999", fmt.Errorf("dial failed")),
			expectedType: ErrConnectionFailed,
			isRetryable:  true,
			isTemporary:  true,
		},
		{
			name:         "max retries exceeded",
			error:        NewMaxRetriesExceededError("node5", "key5", 3, fmt.Errorf("last error")),
			expectedType: ErrMaxRetriesExceeded,
			isRetryable:  false,
			isTemporary:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Check error type
			assert.True(t, errors.Is(tc.error, tc.expectedType), "Expected error type %v", tc.expectedType)

			// Check RouterError type
			var routerErr *RouterError
			assert.True(t, errors.As(tc.error, &routerErr), "Expected RouterError type")

			// Check retryable
			assert.Equal(t, tc.isRetryable, IsRetryableError(tc.error), "IsRetryableError mismatch")

			// Check temporary
			assert.Equal(t, tc.isTemporary, IsTemporaryError(tc.error), "IsTemporaryError mismatch")
		})
	}
}

func TestRouter_IsRoutingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "unavailable error",
			err:      status.Error(codes.Unavailable, "unavailable"),
			expected: true,
		},
		{
			name:     "deadline exceeded error",
			err:      status.Error(codes.DeadlineExceeded, "timeout"),
			expected: true,
		},
		{
			name:     "canceled error",
			err:      status.Error(codes.Canceled, "canceled"),
			expected: true,
		},
		{
			name:     "aborted error",
			err:      status.Error(codes.Aborted, "aborted"),
			expected: true,
		},
		{
			name:     "not found error",
			err:      status.Error(codes.NotFound, "not found"),
			expected: false,
		},
		{
			name:     "circuit breaker error",
			err:      fmt.Errorf("circuit breaker open for node test"),
			expected: false,
		},
		{
			name:     "generic error",
			err:      fmt.Errorf("some error"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsRoutingError(tc.err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
