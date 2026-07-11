// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/client/testutil"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestError_NetworkFailure tests network disconnection handling
func TestError_NetworkFailure(t *testing.T) {
	// Create server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	serverAddr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	server := newMockCacheServiceServer()
	pb.RegisterCacheServiceServer(grpcServer, server)

	// Start server
	go func() {
		grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{serverAddr},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Initial operation should work
	err = client.Put(ctx, "test-key", []byte("value"), 0)
	require.NoError(t, err)

	// Simulate network failure by stopping the server
	grpcServer.Stop()

	// Operations should fail with network error
	err = client.Put(ctx, "test-key-2", []byte("value"), 0)
	assert.Error(t, err)

	// Get should also fail
	_, err = client.Get(ctx, "test-key")
	assert.Error(t, err)
}

// TestError_InvalidResponses tests malformed response handling
func TestError_InvalidResponses(t *testing.T) {
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

	t.Run("InvalidGetRange", func(t *testing.T) {
		// Store data
		server.cacheService.data["range-key"] = []byte("0123456789")

		// Request invalid range (start > end)
		_, err := client.GetRange(ctx, "range-key", 10, 5)
		assert.Error(t, err)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		// Get non-existent key
		_, err := client.Get(ctx, "non-existent-key")
		assert.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

// TestError_StreamingErrors tests error handling in streaming operations
func TestError_StreamingErrors(t *testing.T) {
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

	t.Run("StreamReadError", func(t *testing.T) {
		// Configure server to fail during streaming
		testKey := "stream-error-key"
		server.cacheService.data[testKey] = []byte("0123456789")
		server.InjectErrors(testKey, &errorInjector{
			partialDataBytes: 5,
			networkError:     true,
		})

		// GetStream should fail after partial data
		buf := &testutil.SafeBuffer{}
		err := client.GetStream(ctx, testKey, buf)
		assert.Error(t, err)
		assert.Equal(t, 5, buf.Len()) // Should have received partial data
	})

	t.Run("StreamWriteError", func(t *testing.T) {
		// Use a reader that fails after some data
		failingReader := &testutil.FailingReader{
			Data:      []byte("test data to stream"),
			FailAfter: 10,
		}

		err := client.PutStream(ctx, "stream-write-key", failingReader, 0)
		assert.Error(t, err)
	})

	t.Run("WriterError", func(t *testing.T) {
		// Store data for streaming
		testKey := "writer-error-key"
		server.cacheService.data[testKey] = []byte("test data")

		// Use a writer that fails
		failingWriter := &failingWriter{
			failAfter: 5,
		}

		err := client.GetStream(ctx, testKey, failingWriter)
		assert.Error(t, err)
	})
}

// TestError_ClusterModeErrors tests cluster-specific error scenarios
func TestError_ClusterModeErrors(t *testing.T) {
	t.Run("NoTopologyAvailable", func(t *testing.T) {
		// Create server without topology
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Try to create client in cluster mode (should fail)
		_, err = NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeCluster, // Force cluster mode
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topology")
	})

	t.Run("AllNodesDown", func(t *testing.T) {
		// Create server with topology where all nodes are down
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Set topology with all nodes down
		topology := setupSimpleTopology([]string{server.address})
		for i := range topology.Nodes {
			topology.Nodes[i].Status = clusterpb.NodeStatus_NODE_STATUS_DOWN
		}
		server.cacheService.SetClusterTopology(topology)

		// Client creation might succeed but operations should fail
		client, err := NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeCluster,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		if err == nil {
			defer client.Close()
			// Operations should fail
			ctx := context.Background()
			err = client.Put(ctx, "test-key", []byte("value"), 0)
			assert.Error(t, err)
		}
	})

	t.Run("EmptyTokenRing", func(t *testing.T) {
		// Create server
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()

		// Set topology with empty token assignments
		topology := &clusterpb.ClusterTopology{
			Epoch: 1,
			Nodes: []*clusterpb.NodeInfo{
				{
					Id:            "node-0",
					Address:       server.address,
					ListenAddress: server.address,
					Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
				},
			},
			RingConfig: &clusterpb.RingConfig{
				ReplicationFactor: 1,
				NodeTokens:        []*clusterpb.NodeTokens{}, // No tokens!
			},
		}
		server.cacheService.SetClusterTopology(topology)

		client, err := NewWithConfig(&ClientConfig{
			Addrs: []string{server.address},
			Mode:  ModeCluster,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)
		defer client.Close()

		// Operations should fail due to empty token ring
		ctx := context.Background()
		err = client.Put(ctx, "test-key", []byte("value"), 0)
		assert.Error(t, err)
	})
}

// TestError_Recovery tests error recovery mechanisms
func TestError_Recovery(t *testing.T) {
	// Create two servers
	server1, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server2.Stop()

	// Create client with both servers
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server1.address, server2.address},
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Put data on both servers (for simple mode testing)
	testKey := "recovery-key"
	testValue := []byte("recovery-value")
	server1.cacheService.data[testKey] = testValue
	server2.cacheService.data[testKey] = testValue

	// Verify operations work
	data, err := client.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, testValue, data)

	// Make server1 return errors
	server1.cacheService.getError = status.Error(codes.Internal, "server error")

	// Some operations should still succeed (those that route to server2)
	successes := 0
	failures := 0
	for i := 0; i < 20; i++ {
		// Use different keys to test routing
		key := "test-key-" + string(rune('0'+i))
		server2.cacheService.data[key] = []byte("value")

		_, err := client.Get(ctx, key)
		if err != nil {
			failures++
		} else {
			successes++
		}
	}

	// Some operations should succeed (those routed to server2)
	assert.Greater(t, successes, 0, "Some operations should succeed")
	assert.Greater(t, failures, 0, "Some operations should fail")
	t.Logf("Recovery test: %d successes, %d failures", successes, failures)
}

// TestError_ClusterDegradation tests cluster behavior with progressive node failures
func TestError_ClusterDegradation(t *testing.T) {
	// Create three servers for cluster
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := 0; i < 3; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Set initial topology
	topology := setupSimpleTopology(addresses)
	for _, server := range servers {
		server.cacheService.SetClusterTopology(topology)
		server.cacheService.nodeID = "" // Disable ownership checks
	}

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{servers[0].address},
		Mode:            ModeCluster,
		RefreshInterval: 100 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Verify initial operations work
	err = client.Put(ctx, "degrade-key", []byte("value"), 0)
	require.NoError(t, err)

	// Simulate one server failure
	servers[1].Stop()

	// Update topology to remove the down node
	topology2 := setupSimpleTopology([]string{addresses[0], addresses[2]})
	topology2.Epoch = 2
	servers[0].cacheService.SetClusterTopology(topology2)
	servers[2].cacheService.SetClusterTopology(topology2)

	// Wait for topology refresh
	time.Sleep(200 * time.Millisecond)

	// Operations should continue with remaining servers
	err = client.Put(ctx, "degrade-key-2", []byte("value"), 0)
	assert.NoError(t, err, "Operations should continue in degraded mode")

	// Check that client updated its node list
	connectedNodes := client.GetConnectedNodes()
	assert.Len(t, connectedNodes, 2, "Should have 2 connected nodes after one failure")
}
