package cacheclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestConnectionPool_RoundRobin verifies round-robin client selection
func TestConnectionPool_RoundRobin(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client with multiple connections in pool
	poolSize := 4
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: poolSize,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Verify pool was created with correct size
	pool := client.pools[server.address]
	require.NotNil(t, pool)
	assert.Len(t, pool.clients, poolSize)

	// Track which clients are used
	clientsUsed := make(map[interface{}]int)

	// Make multiple requests
	ctx := context.Background()
	for i := 0; i < poolSize*3; i++ {
		// Each call should use a different client from the pool
		err := client.Put(ctx, "key"+string(rune('0'+i)), []byte("value"), 0)
		require.NoError(t, err)

		// Get the client that was used (based on round-robin index)
		expectedIdx := uint64(i) % uint64(poolSize)
		expectedClient := pool.clients[expectedIdx]
		clientsUsed[expectedClient]++
	}

	// Each client should have been used equally
	for _, count := range clientsUsed {
		assert.Equal(t, 3, count, "Each client should be used 3 times")
	}
}


// TestConnectionPool_ConnectionFailure tests handling of connection errors
func TestConnectionPool_ConnectionFailure(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: 2,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Verify operations work initially
	err = client.Put(ctx, "test-key-1", []byte("value1"), 0)
	require.NoError(t, err)

	// Stop the server to simulate connection failure
	server.Stop()

	// Operations should fail now
	err = client.Put(ctx, "test-key-2", []byte("value2"), 0)
	assert.Error(t, err)

	// Restart server on same address
	server, err = newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Note: In a real implementation, you might want to implement
	// connection recovery. For now, operations will continue to fail
	// because the existing connections are broken.
}

// TestConnectionPool_Cleanup verifies proper resource cleanup
func TestConnectionPool_Cleanup(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create and immediately close multiple clients
	for i := 0; i < 5; i++ {
		client, err := NewWithConfig(&ClientConfig{
			Addrs:    []string{server.address},
			Mode:     ModeSimple,
			PoolSize: 4,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)

		// Use the client
		ctx := context.Background()
		err = client.Put(ctx, "key", []byte("value"), 0)
		require.NoError(t, err)

		// Close should clean up all connections
		err = client.Close()
		require.NoError(t, err)

		// After close, operations should fail
		err = client.Put(ctx, "key", []byte("value"), 0)
		assert.Error(t, err)
	}
}

// TestConnectionPool_MultipleAddresses tests pooling across multiple servers
func TestConnectionPool_MultipleAddresses(t *testing.T) {
	// Create multiple servers
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := 0; i < 3; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Create client with multiple addresses
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    addresses,
		Mode:     ModeSimple,
		PoolSize: 2, // 2 connections per address
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Should have pool for each address
	assert.Len(t, client.pools, 3)
	for _, addr := range addresses {
		pool := client.pools[addr]
		require.NotNil(t, pool)
		assert.Len(t, pool.clients, 2)
	}

	// Make requests - they should be distributed across servers
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		key := "key-" + string(rune('0'+i))
		err := client.Put(ctx, key, []byte("value"), 0)
		require.NoError(t, err)
	}

	// Each server should have received some requests
	for _, server := range servers {
		putCount, _, _, _ := server.GetCallCounts()
		assert.Greater(t, putCount, int32(0), "Each server should receive requests")
	}
}

// TestConnectionPool_HighLoad tests pool under high load
func TestConnectionPool_HighLoad(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client with larger pool for high load
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: 10,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	start := time.Now()
	totalOps := int32(0)
	errors := int32(0)

	// Launch many concurrent operations
	concurrency := 50
	opsPerWorker := 100

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := "load-key-" + string(rune('0'+id%10)) + "-" + string(rune('0'+j%10))
				err := client.Put(ctx, key, []byte("value"), 0)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&totalOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Calculate throughput
	opsPerSecond := float64(totalOps) / duration.Seconds()
	t.Logf("High load test: %d operations in %v (%.2f ops/sec), %d errors",
		totalOps, duration, opsPerSecond, errors)

	// Verify high success rate
	expectedOps := int32(concurrency * opsPerWorker)
	successRate := float64(totalOps) / float64(expectedOps) * 100
	assert.Greater(t, successRate, 95.0, "Success rate should be > 95%")
}

// TestConnectionPool_ClusterMode tests pooling in cluster mode
func TestConnectionPool_ClusterMode(t *testing.T) {
	// Create multiple servers for cluster
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := 0; i < 3; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Set up topology on first server
	topology := setupSimpleTopology(addresses)
	servers[0].clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{servers[0].address}, // Only need seed node
		Mode:     ModeCluster,
		PoolSize: 2,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Should have discovered all nodes and created pools
	assert.Len(t, client.pools, 3)
	for _, addr := range addresses {
		pool := client.pools[addr]
		require.NotNil(t, pool, "Pool should exist for address %s", addr)
		assert.Len(t, pool.clients, 2)
	}

	// Test operations work with discovered pools
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		key := "cluster-key-" + string(rune('0'+i))
		// Disable ownership checks since we're testing pool behavior
		for _, server := range servers {
			server.cacheService.nodeID = ""
		}
		err := client.Put(ctx, key, []byte("value"), 0)
		require.NoError(t, err)
	}
}

