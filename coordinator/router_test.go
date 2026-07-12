// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// mockRing implements the Ring interface for testing
type mockRing struct {
	nodes       map[string]*ring.NodeInfo
	keyToNode   map[string]string // key -> nodeID
	localNodeID string
	mu          sync.RWMutex
}

func newMockRing(localNodeID string) *mockRing {
	return &mockRing{
		nodes:       make(map[string]*ring.NodeInfo),
		keyToNode:   make(map[string]string),
		localNodeID: localNodeID,
	}
}

func (m *mockRing) AddNode(id, address, listenAddress string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = &ring.NodeInfo{
		ID:            id,
		Address:       address,
		ListenAddress: listenAddress,
		Status:        ring.NodeStatusActive,
		Available:     true,
	}
}

func (m *mockRing) RemoveNode(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
}

func (m *mockRing) SetKeyOwner(key, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyToNode[key] = nodeID
}

func (m *mockRing) GetNode(key string) (*ring.NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if nodeID, exists := m.keyToNode[key]; exists {
		if node, ok := m.nodes[nodeID]; ok {
			return node, nil
		}
	}
	// Default: return first node
	for _, node := range m.nodes {
		return node, nil
	}
	return nil, fmt.Errorf("no node available for key %s", key)
}

func (m *mockRing) GetAllNodes() []*ring.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*ring.NodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (m *mockRing) GetActiveNodes() []*ring.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*ring.NodeInfo, 0)
	for _, node := range m.nodes {
		if node.Status == ring.NodeStatusActive {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (m *mockRing) IsLocal(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if nodeID, exists := m.keyToNode[key]; exists {
		return nodeID == m.localNodeID
	}
	return false
}

// mockRouterClusterService implements a minimal ClusterServiceServer for router testing
type mockRouterClusterService struct {
	clusterpb.UnimplementedClusterServiceServer
	failUntil time.Time
	nodeID    string
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

func (m *mockRouterCacheService) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return &pb.ListResponse{Keys: []string{"key1", "key2"}}, nil
}

func (m *mockRouterCacheService) ListLocal(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return &pb.ListResponse{Keys: []string{"key1", "key2"}}, nil
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
	// Create mock ring
	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "localhost:7000", "localhost:9000")

	// Start mock servers on different ports
	// Cache service on port that will be used as listen address
	cacheLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer cacheLis.Close()

	// Cluster service on port that will be used as cluster address
	clusterLis, err := net.Listen("tcp", "localhost:0")
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
	mockRing.AddNode("remote-node", clusterLis.Addr().String(), cacheLis.Addr().String())
	mockRing.SetKeyOwner("test-key", "remote-node")

	// Create router
	router := NewRouter(mockRing, "local-node")
	defer router.Close()

	// Route to remote node
	client, err := router.Route("test-key")
	require.NoError(t, err)
	require.NotNil(t, client)

	// Make a request
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: "test-key"})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify the correct server was called
	assert.True(t, cacheServiceCalled, "Cache service on listen address should be called")
	assert.False(t, clusterServiceCalled, "Cluster service on cluster address should NOT be called")
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
	// Create mock ring and add nodes
	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "localhost:7999", "localhost:9999")

	// Start mock remote node
	cacheAddr, clusterAddr, grpcClusterServer, grpcCacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcClusterServer.Stop()
	defer grpcCacheServer.Stop()

	mockRing.AddNode("remote-node", clusterAddr, cacheAddr)

	// Set up key ownership
	mockRing.SetKeyOwner("local-key", "local-node")
	mockRing.SetKeyOwner("remote-key", "remote-node")

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
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	// Test routing to local node (should return error)
	client, err := router.Route("local-key")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, errors.Is(err, ErrLocalRouting), "Expected ErrLocalRouting")
	var routerErr *RouterError
	assert.True(t, errors.As(err, &routerErr), "Expected RouterError type")
	assert.Equal(t, "local-node", routerErr.NodeID)
	assert.Equal(t, "local-key", routerErr.Key)

	// Test routing to remote node
	client, err = router.Route("remote-key")
	assert.NoError(t, err)
	assert.NotNil(t, client, "Remote routing should return a client")
}

func TestRouter_CircuitBreaker(t *testing.T) {
	// Create mock ring
	mockRing := newMockRing("local-node")

	// Add a non-existent node
	mockRing.AddNode("dead-node", "localhost:77777", "localhost:99999")
	mockRing.SetKeyOwner("dead-key", "dead-node")

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
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	// Try multiple times to trigger circuit breaker
	for i := 0; i < config.CircuitBreakerThreshold; i++ {
		_, err := router.Route("dead-key")
		// Expect error due to connection failure
		assert.Error(t, err)
	}

	// Circuit should be open now
	_, err := router.Route("dead-key")
	require.Error(t, err, "Expected error when circuit breaker is open")
	assert.True(t, errors.Is(err, ErrCircuitBreakerOpen) || errors.Is(err, ErrMaxRetriesExceeded),
		"Expected circuit breaker or max retries error")

	// Wait for circuit breaker timeout
	time.Sleep(config.CircuitBreakerTimeout + 100*time.Millisecond)

	// Circuit should be closed now, should be able to try again
	_, err = router.Route("dead-key")
	// Won't get circuit breaker error immediately after reset
	if err != nil {
		assert.False(t, errors.Is(err, ErrCircuitBreakerOpen), "Circuit breaker should be closed after timeout")
	}
}

func TestRouter_RemoveClient(t *testing.T) {
	// Create mock ring
	mockRing := newMockRing("local-node")

	// Start mock remote node
	listenAddr, clusterAddr, grpcClusterServer, grpcCacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcClusterServer.Stop()
	defer grpcCacheServer.Stop()

	mockRing.AddNode("remote-node", clusterAddr, listenAddr)
	mockRing.SetKeyOwner("remote-key", "remote-node")

	router := NewRouter(mockRing, "local-node")
	defer router.Close()

	// Create connection
	client, err := router.Route("remote-key")
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Remove client
	router.RemoveClient("remote-node")

	// Should create new connection
	client2, err := router.Route("remote-key")
	assert.NoError(t, err)
	assert.NotNil(t, client2)
}

func TestRouter_RefreshConnections(t *testing.T) {
	// Create mock ring
	mockRing := newMockRing("local-node")

	// Start two mock nodes
	addr1, clusterAddr1, server1, cacheServer1, _, _ := startMockRouterServer(t, "node1")
	defer server1.Stop()
	defer cacheServer1.Stop()
	addr2, clusterAddr2, server2, cacheServer2, _, _ := startMockRouterServer(t, "node2")
	defer server2.Stop()
	defer cacheServer2.Stop()

	mockRing.AddNode("node1", clusterAddr1, addr1)
	mockRing.AddNode("node2", clusterAddr2, addr2)
	mockRing.SetKeyOwner("key1", "node1")
	mockRing.SetKeyOwner("key2", "node2")

	router := NewRouter(mockRing, "local-node")
	defer router.Close()

	// Create connections to both nodes
	router.Route("key1")
	router.Route("key2")

	// Remove node2 from ring
	mockRing.RemoveNode("node2")

	// Refresh connections
	router.RefreshConnections()

	// Check that only node1 connection remains
	stats := router.GetConnectionStats()
	assert.Contains(t, stats, "node1")
	assert.NotContains(t, stats, "node2")
}

func TestRouter_GetConnectionStats(t *testing.T) {
	// Create mock ring
	mockRing := newMockRing("local-node")

	// Start mock node
	listenAddr, clusterAddr, grpcClusterServer, grpcCacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer grpcClusterServer.Stop()
	defer grpcCacheServer.Stop()

	mockRing.AddNode("remote-node", clusterAddr, listenAddr)
	mockRing.SetKeyOwner("remote-key", "remote-node")

	router := NewRouter(mockRing, "local-node")
	defer router.Close()

	// Initially no stats
	stats := router.GetConnectionStats()
	assert.Empty(t, stats)

	// Create connection
	_, err := router.Route("remote-key")
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
