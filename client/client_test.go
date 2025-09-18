package cacheclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
		server.clusterService.SetTopology(topology)

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
