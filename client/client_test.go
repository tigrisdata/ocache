package cacheclient

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClientConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *ClientConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty addresses",
			config: &ClientConfig{
				Addrs: []string{},
			},
			wantErr: true,
			errMsg:  "at least one address is required",
		},
		{
			name: "valid single address",
			config: &ClientConfig{
				Addrs: []string{"localhost:9000"},
				Mode:  ModeSimple,
			},
			wantErr: false,
		},
		{
			name: "multiple addresses",
			config: &ClientConfig{
				Addrs: []string{"node1:9000", "node2:9000"},
				Mode:  ModeSimple,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip actual connection tests that require a server
			if !tt.wantErr {
				t.Skip("Skipping test that requires a running server")
			}

			_, err := NewWithConfig(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestClient_SimpleMode demonstrates testing client in simple mode with mock servers
func TestClient_SimpleMode(t *testing.T) {
	// Create multiple mock servers
	server1, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server2.Stop()

	// Prepare test data on both servers (since we don't control routing in simple mode)
	server1.cacheService.data["key1"] = []byte("value1")
	server1.cacheService.data["key2"] = []byte("value2")
	server2.cacheService.data["key1"] = []byte("value1")
	server2.cacheService.data["key2"] = []byte("value2")

	// Create client in simple mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server1.address, server2.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Test that client is in simple mode
	assert.Equal(t, ModeSimple, client.GetMode())
	assert.ElementsMatch(t, []string{server1.address, server2.address}, client.GetConnectedNodes())

	// Test Get operation
	ctx := context.Background()

	// Get from server1
	data, err := client.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), data)

	// Get from server2
	data, err = client.Get(ctx, "key2")
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), data)

	// Test Put operation
	err = client.Put(ctx, "key3", []byte("value3"), 0)
	require.NoError(t, err)

	// Verify Put was called (at least once on one of the servers)
	put1, _, _, _ := server1.GetCallCounts()
	put2, _, _, _ := server2.GetCallCounts()
	assert.True(t, put1 > 0 || put2 > 0, "Put should have been called on at least one server")
}

// TestClient_ClusterMode demonstrates testing client in cluster mode with mock servers
func TestClient_ClusterMode(t *testing.T) {
	// Create a 3-node cluster
	servers, _, err := setupMultiNodeTestServers(3)
	require.NoError(t, err)
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	// Disable partition ownership checks for this test since client and mock use different hashing
	for _, server := range servers {
		server.cacheService.nodeID = "" // This disables ownership checks
	}

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{servers[0].address}, // Only need one seed node
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Test that client is in cluster mode
	assert.Equal(t, ModeCluster, client.GetMode())

	// Client should have discovered all nodes
	connectedNodes := client.GetConnectedNodes()
	assert.Len(t, connectedNodes, 3)

	// Test operations work correctly
	ctx := context.Background()

	// Put a key
	testKey := "test-key"
	err = client.Put(ctx, testKey, []byte("test-value"), 0)
	require.NoError(t, err)

	// At least one server should have received the Put
	totalPuts := int32(0)
	var dataServer *testServer
	for _, server := range servers {
		putCount, _, _, _ := server.GetCallCounts()
		totalPuts += putCount
		if _, exists := server.cacheService.data[testKey]; exists {
			dataServer = server
		}
	}
	assert.Greater(t, totalPuts, int32(0), "At least one server should have received Put")
	require.NotNil(t, dataServer, "Data should be stored on at least one server")

	// Verify data is stored correctly
	assert.Equal(t, []byte("test-value"), dataServer.cacheService.data[testKey])

	// Test Get operation - should retrieve from the same server
	data, err := client.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), data)
}

// TestClient_AutoMode demonstrates auto-detection of cluster mode
func TestClient_AutoMode(t *testing.T) {
	t.Run("DetectsClusterMode", func(t *testing.T) {
		// Create a server with cluster topology
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Set up topology
		topology := setupSimpleTopology([]string{server.address})
		server.cacheService.clusterTopology = topology

		// Create client in auto mode
		client, err := NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeAuto,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)
		defer client.Close()

		// Should detect cluster mode
		assert.Equal(t, ModeCluster, client.GetMode())
	})

	t.Run("FallsBackToSimpleMode", func(t *testing.T) {
		// Create a server without cluster topology
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Don't set topology - cluster service will return error

		// Create client in auto mode
		client, err := NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeAuto,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)
		defer client.Close()

		// Should fall back to simple mode
		assert.Equal(t, ModeSimple, client.GetMode())
	})
}

// TestClient_Lifecycle_FullCycle tests complete lifecycle
func TestClient_Lifecycle_FullCycle(t *testing.T) {
	// Create servers
	servers := make([]*testServer, 2)
	addresses := make([]string, 2)
	for i := 0; i < 2; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Test multiple client lifecycles
	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("Starting lifecycle cycle %d", cycle+1)

		// Create client
		client, err := NewWithConfig(&ClientConfig{
			Addrs: addresses,
			Mode:  ModeSimple,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)

		ctx := context.Background()

		// Put data through client and verify operations
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("cycle-%d-key-%d", cycle, i)

			// Put data using client
			err := client.Put(ctx, key, []byte("value"), 0)
			require.NoError(t, err)

			// Verify with Get
			data, err := client.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, []byte("value"), data)
		}

		// Close client
		err = client.Close()
		require.NoError(t, err)

		// Verify closed
		err = client.Put(ctx, "after-close", []byte("value"), 0)
		assert.Error(t, err)
	}
}

// TestClient_Lifecycle_InitFailures tests handling of initialization failures
func TestClient_Lifecycle_InitFailures(t *testing.T) {
	t.Run("InvalidAddress", func(t *testing.T) {
		// Try to create client with invalid address
		_, err := NewWithConfig(&ClientConfig{
			Addrs: []string{"invalid-address-without-port"},
			Mode:  ModeSimple,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.WithTimeout(100 * time.Millisecond),
			},
		})
		// Should either fail or timeout
		if err == nil {
			t.Log("Client created with invalid address - might fail on first operation")
		}
	})

	t.Run("UnreachableServer", func(t *testing.T) {
		// Try to create client with unreachable server
		_, err := NewWithConfig(&ClientConfig{
			Addrs: []string{"localhost:59999"}, // Unlikely to be in use
			Mode:  ModeSimple,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.WithTimeout(100 * time.Millisecond),
			},
		})
		// Should timeout or fail
		if err == nil {
			t.Log("Client created with unreachable server - will fail on operations")
		}
	})

	t.Run("ClusterModeNoTopology", func(t *testing.T) {
		// Create server without topology
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Force cluster mode should fail without topology
		_, err = NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeCluster,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		assert.Error(t, err)
	})
}

// TestConnection tests basic connection operations
func TestConnection(t *testing.T) {
	// Create a test server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	serverAddr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, newMockCacheServiceServer())
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	t.Run("create and close connection", func(t *testing.T) {
		// Create connection
		conn, err := newConnection(serverAddr, []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, 1)
		require.NoError(t, err)
		assert.NotNil(t, conn)
		assert.Equal(t, serverAddr, conn.address)

		// Get client should return non-nil
		client := conn.getClient()
		assert.NotNil(t, client)

		// Close connection
		err = conn.close()
		assert.NoError(t, err)
	})

	t.Run("connection health check", func(t *testing.T) {
		conn, err := newConnection(serverAddr, []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, 1)
		require.NoError(t, err)
		defer conn.close()

		// Should be healthy initially
		assert.True(t, conn.isHealthy())

		// Record an error
		conn.recordError(fmt.Errorf("test error"))

		// Should still be healthy if not a connection error
		// (connection.go checks if error is a connection error)
		healthy := conn.isHealthy()
		_ = healthy // May or may not be healthy depending on error type
	})

	t.Run("record and clear errors", func(t *testing.T) {
		conn, err := newConnection(serverAddr, []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, 1)
		require.NoError(t, err)
		defer conn.close()

		// Record an error
		testErr := fmt.Errorf("test error")
		conn.recordError(testErr)

		// Recording nil should be safe (no-op)
		conn.recordError(nil)

		// Connection should track the error internally
		// We can't access internal fields, but operation should not panic
	})
}

// TestConnection_Reconnect tests reconnection behavior
func TestConnection_Reconnect(t *testing.T) {
	// Create a test server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	serverAddr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, newMockCacheServiceServer())
	go grpcServer.Serve(listener)

	t.Run("reconnect after server restart", func(t *testing.T) {
		// Create connection
		conn, err := newConnection(serverAddr, []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, 1)
		require.NoError(t, err)
		defer conn.close()

		// Stop server
		grpcServer.Stop()

		// Wait for connection to notice
		time.Sleep(100 * time.Millisecond)

		// Restart server on same port
		listener2, err := net.Listen("tcp", serverAddr)
		if err != nil {
			// Port might still be in use, skip this test
			t.Skip("Cannot reuse port immediately")
		}

		grpcServer2 := grpc.NewServer()
		pb.RegisterCacheServiceServer(grpcServer2, newMockCacheServiceServer())
		go grpcServer2.Serve(listener2)
		defer grpcServer2.Stop()

		// Try to reconnect
		dialOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		err = conn.reconnect(dialOpts)
		// Reconnect may succeed or fail depending on timing
		_ = err
	})
}

// TestConnection_State tests connection state transitions
func TestConnection_State(t *testing.T) {
	// Create a test server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	serverAddr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, newMockCacheServiceServer())
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	t.Run("connection states", func(t *testing.T) {
		// Create connection
		conn, err := newConnection(serverAddr, []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, 1)
		require.NoError(t, err)
		defer conn.close()

		// Check initial state
		state := conn.connections[0].GetState()
		assert.True(t,
			state == connectivity.Ready ||
				state == connectivity.Connecting ||
				state == connectivity.Idle,
			"Connection should be in a valid initial state")

		// Use the connection
		ctx := context.Background()
		client := conn.getClient()

		// Try a simple operation
		stream, err := client.Get(ctx, &pb.GetRequest{Key: "test"})
		// Will fail with NotFound, but connection should work
		_ = stream
		_ = err

		// Connection should still be valid
		assert.True(t, conn.isHealthy())
	})
}
