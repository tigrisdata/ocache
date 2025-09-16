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

func startMockRouterServer(t *testing.T, nodeID string) (string, *grpc.Server, *mockRouterClusterService) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	mockService := &mockRouterClusterService{nodeID: nodeID}
	clusterpb.RegisterClusterServiceServer(grpcServer, mockService)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Mock server stopped: %v", err)
		}
	}()

	return lis.Addr().String(), grpcServer, mockService
}

func TestRouter_BasicRouting(t *testing.T) {
	// Create ring and add nodes
	ring, err := NewRing(100, "local-node")
	require.NoError(t, err)

	// Add self to ring first (coordinator does this)
	err = ring.AddNode("local-node", "localhost:9999")
	require.NoError(t, err)

	// Start mock remote node
	remoteAddr, grpcServer, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	err = ring.AddNode("remote-node", remoteAddr)
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
	remoteAddr, grpcServer, mockService := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	// Make the service fail for 100ms
	mockService.failUntil = time.Now().Add(100 * time.Millisecond)

	err = ring.AddNode("remote-node", remoteAddr)
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
	err = ring.AddNode("dead-node", "localhost:99999")
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
	remoteAddr, grpcServer, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	err = ring.AddNode("remote-node", remoteAddr)
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
	addr1, server1, _ := startMockRouterServer(t, "node1")
	defer server1.Stop()
	addr2, server2, _ := startMockRouterServer(t, "node2")
	defer server2.Stop()

	err = ring.AddNode("node1", addr1)
	require.NoError(t, err)
	err = ring.AddNode("node2", addr2)
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
	remoteAddr, grpcServer, _ := startMockRouterServer(t, "remote-node")
	defer grpcServer.Stop()

	err = ring.AddNode("remote-node", remoteAddr)
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
