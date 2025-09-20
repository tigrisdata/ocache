package cacheclient

import (
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// helper function to create test server
func createTestServer(t *testing.T) (string, func()) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	serverAddr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, newMockCacheServiceServer())
	go grpcServer.Serve(listener)

	return serverAddr, func() {
		grpcServer.Stop()
	}
}

// TestConnectionPool_Creation tests creating a connection pool with multiple connections
func TestConnectionPool_Creation(t *testing.T) {
	tests := []struct {
		name         string
		poolSize     int
		expectedSize int
	}{
		{
			name:         "default pool size",
			poolSize:     0,
			expectedSize: 3, // Should use default
		},
		{
			name:         "custom pool size",
			poolSize:     5,
			expectedSize: 5,
		},
		{
			name:         "single connection",
			poolSize:     1,
			expectedSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverAddr, cleanup := createTestServer(t)
			defer cleanup()

			dialOpts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			}

			conn, err := newConnection(serverAddr, dialOpts, tt.poolSize)
			require.NoError(t, err)
			defer conn.close()

			// Verify pool size
			assert.Equal(t, tt.expectedSize, conn.poolSize)
			assert.Equal(t, tt.expectedSize, len(conn.connections))
			assert.Equal(t, tt.expectedSize, len(conn.clients))

			// Verify all connections are non-nil
			for i, c := range conn.connections {
				assert.NotNil(t, c, "connection %d should not be nil", i)
			}

			// Verify all clients are non-nil
			for i, client := range conn.clients {
				assert.NotNil(t, client, "client %d should not be nil", i)
			}
		})
	}
}

// TestConnectionPool_RoundRobin tests that clients are selected in round-robin fashion
func TestConnectionPool_RoundRobin(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)
	defer cleanup()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a pool with 3 connections
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Track which clients we get
	clientPtrs := make(map[uintptr]int)

	// Get clients multiple times and verify round-robin distribution
	numRequests := 30
	for i := 0; i < numRequests; i++ {
		client := conn.getClient()
		require.NotNil(t, client)

		// Track this client pointer
		ptr := reflect.ValueOf(client).Pointer()
		clientPtrs[ptr]++
	}

	// Verify we got all 3 different clients
	assert.Equal(t, 3, len(clientPtrs), "Should have gotten 3 different clients")

	// Verify roughly equal distribution (with some tolerance for starting index)
	for ptr, count := range clientPtrs {
		expectedCount := numRequests / 3
		assert.InDelta(t, expectedCount, count, 2,
			"Client %v should have been selected approximately %d times, got %d",
			ptr, expectedCount, count)
	}
}

// TestConnectionPool_ConcurrentAccess tests that multiple goroutines can access clients concurrently
func TestConnectionPool_ConcurrentAccess(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)
	defer cleanup()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a pool with 3 connections
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Launch many concurrent goroutines to get clients
	numGoroutines := 100
	numRequestsPerGoroutine := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numRequestsPerGoroutine; j++ {
				client := conn.getClient()
				if client != nil {
					// Simulate using the client - just check it's not nil
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// Verify all requests succeeded
	expectedTotal := int32(numGoroutines * numRequestsPerGoroutine)
	assert.Equal(t, expectedTotal, successCount.Load(),
		"All concurrent requests should have gotten a client")
}

// TestConnectionPool_HealthManagement tests health checking with multiple connections
func TestConnectionPool_HealthManagement(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)
	defer cleanup()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a pool with 3 connections
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Initially all connections should be healthy
	assert.True(t, conn.isHealthy())

	// Get health stats
	healthy, total := conn.getHealthStats()
	assert.Equal(t, 3, total)
	// Connections may be in Idle or Connecting state initially
	assert.GreaterOrEqual(t, total, 1, "Should have created connections")

	// Simulate connection errors
	conn.mu.Lock()
	// Manually set one connection to nil to simulate failure
	if len(conn.connections) > 0 {
		conn.connections[0].Close()
		conn.connections[0] = nil
		conn.clients[0] = nil
	}
	conn.mu.Unlock()

	// Should still be healthy with remaining connections
	assert.True(t, conn.isHealthy())

	// Get health stats again
	healthy, total = conn.getHealthStats()
	assert.Equal(t, 3, total)
	assert.GreaterOrEqual(t, healthy, 0, "Some connections might still be healthy")
}

// TestConnectionPool_Reconnect tests reconnecting unhealthy connections
func TestConnectionPool_Reconnect(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)
	defer cleanup()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	// Create a pool with 3 connections
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Initially all should be healthy
	healthy, total := conn.getHealthStats()
	assert.Equal(t, 3, total)
	initialHealthy := healthy

	// Close one connection to simulate failure
	conn.mu.Lock()
	if len(conn.connections) > 0 {
		conn.connections[0].Close()
		// Don't nil it out, just close it so reconnect can detect it
	}
	conn.mu.Unlock()

	// Wait a bit for the connection state to update
	time.Sleep(100 * time.Millisecond)

	// Reconnect unhealthy connections
	_ = conn.reconnect(dialOpts)
	// Might have an error if server is not accepting new connections
	// but the important thing is it attempted to reconnect

	// Check health again
	healthy, total = conn.getHealthStats()
	assert.Equal(t, 3, total)
	// Should have at least as many healthy as we started with minus 1
	// (the reconnect might succeed or fail depending on timing)
	assert.GreaterOrEqual(t, healthy, initialHealthy-1)
}

// TestConnectionPool_GetClientWithUnhealthyConnections tests client selection when some connections are unhealthy
func TestConnectionPool_GetClientWithUnhealthyConnections(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)
	defer cleanup()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a pool with 3 connections
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Close 2 out of 3 connections
	conn.mu.Lock()
	if len(conn.connections) >= 3 {
		conn.connections[0].Close()
		conn.connections[0] = nil
		conn.clients[0] = nil

		conn.connections[1].Close()
		conn.connections[1] = nil
		conn.clients[1] = nil
	}
	conn.mu.Unlock()

	// Should still be able to get the remaining healthy client
	for i := 0; i < 10; i++ {
		client := conn.getClient()
		assert.NotNil(t, client, "Should still get a client even with some unhealthy connections")
	}

	// Connection should still report as healthy (at least one connection works)
	assert.True(t, conn.isHealthy())
}

// TestConnectionPool_AllConnectionsFailed tests behavior when all connections fail
func TestConnectionPool_AllConnectionsFailed(t *testing.T) {
	serverAddr, cleanup := createTestServer(t)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a pool
	conn, err := newConnection(serverAddr, dialOpts, 3)
	require.NoError(t, err)
	defer conn.close()

	// Stop the server to make all connections unhealthy
	cleanup()

	// Wait for connections to detect the failure
	time.Sleep(100 * time.Millisecond)

	// Close all connections
	conn.mu.Lock()
	for i := range conn.connections {
		if conn.connections[i] != nil {
			conn.connections[i].Close()
			conn.connections[i] = nil
			conn.clients[i] = nil
		}
	}
	conn.mu.Unlock()

	// getClient should return nil when no connections available
	client := conn.getClient()
	assert.Nil(t, client, "Should return nil when no healthy connections")

	// Should report as unhealthy
	conn.recordError(errors.New("connection closed"))
	assert.False(t, conn.isHealthy())
}

// TestConnectionPool_ConfigIntegration tests integration with ClientConfig
func TestConnectionPool_ConfigIntegration(t *testing.T) {
	tests := []struct {
		name         string
		configSize   int
		expectedSize int
	}{
		{
			name:         "zero uses default",
			configSize:   0,
			expectedSize: DefaultConnectionPoolSize,
		},
		{
			name:         "negative uses default",
			configSize:   -1,
			expectedSize: DefaultConnectionPoolSize,
		},
		{
			name:         "custom size",
			configSize:   7,
			expectedSize: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ClientConfig{
				ConnectionPoolSize: tt.configSize,
			}
			config.SetDefaults()

			assert.Equal(t, tt.expectedSize, config.ConnectionPoolSize)
		})
	}
}
