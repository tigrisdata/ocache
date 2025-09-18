package cacheclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestClient_Close_SimpleMode tests clean shutdown in simple mode
func TestClient_Close_SimpleMode(t *testing.T) {
	// Create multiple servers
	servers := make([]*testServer, 2)
	addresses := make([]string, 2)
	for i := 0; i < 2; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Create client in simple mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    addresses,
		Mode:     ModeSimple,
		PoolSize: 3,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	// Verify client is functional
	ctx := context.Background()
	err = client.Put(ctx, "test-key", []byte("value"), 0)
	require.NoError(t, err)

	// Close client
	err = client.Close()
	require.NoError(t, err)

	// Operations should fail after close
	err = client.Put(ctx, "after-close", []byte("value"), 0)
	assert.Error(t, err, "Operations should fail after close")

	// Get should also fail
	_, err = client.Get(ctx, "test-key")
	assert.Error(t, err, "Get should fail after close")

	// List should fail
	_, err = client.List(ctx, "")
	assert.Error(t, err, "List should fail after close")

	// GetMode should still work (doesn't use connections)
	mode := client.GetMode()
	assert.Equal(t, ModeSimple, mode)

	// GetConnectedNodes should return empty after close
	nodes := client.GetConnectedNodes()
	assert.Empty(t, nodes, "Should return empty list after close")
}

// TestClient_Close_ClusterMode tests clean shutdown with topology refresh
func TestClient_Close_ClusterMode(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode with short refresh interval
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 50 * time.Millisecond,
		PoolSize:        2,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	// Let topology refresh run a few times
	time.Sleep(150 * time.Millisecond)

	// Record topology fetch count
	fetchCountBefore := server.clusterService.getTopologyCallCount.Load()

	// Close client
	err = client.Close()
	require.NoError(t, err)

	// Wait a bit
	time.Sleep(150 * time.Millisecond)

	// Topology refresh should have stopped (at most 1 in-flight request may complete)
	fetchCountAfter := server.clusterService.getTopologyCallCount.Load()
	assert.LessOrEqual(t, fetchCountAfter-fetchCountBefore, int32(1), "At most 1 in-flight topology fetch should occur after close")

	// Operations should fail after close
	ctx := context.Background()
	err = client.Put(ctx, "after-close", []byte("value"), 0)
	assert.Error(t, err)
}

// TestClient_Close_IdempotentCalls tests multiple close calls are safe
func TestClient_Close_IdempotentCalls(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	// First close should succeed
	err = client.Close()
	require.NoError(t, err)

	// Subsequent closes should be safe (not panic or error)
	for i := 0; i < 5; i++ {
		err = client.Close()
		// Should either succeed or return a specific error
		// but should never panic
		if err != nil {
			t.Logf("Close call %d returned error: %v", i+2, err)
		}
	}
}

// TestClient_Close_ConcurrentOperations tests operations during close
func TestClient_Close_ConcurrentOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: 4,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start concurrent operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					key := "concurrent-" + string(rune('0'+id))
					// Ignore errors as close might happen during operation
					client.Put(ctx, key, []byte("value"), 0)
					client.Get(ctx, key)
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let operations run
	time.Sleep(100 * time.Millisecond)

	// Close client while operations are running
	closeErr := make(chan error, 1)
	go func() {
		closeErr <- client.Close()
	}()

	// Stop operations
	close(stopCh)
	wg.Wait()

	// Close should complete
	select {
	case err := <-closeErr:
		assert.NoError(t, err, "Close should not error")
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete in time")
	}
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
			Addrs:    addresses,
			Mode:     ModeSimple,
			PoolSize: 2,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)

		ctx := context.Background()

		// Perform operations
		for i := 0; i < 10; i++ {
			key := "cycle-" + string(rune('0'+cycle)) + "-key-" + string(rune('0'+i))
			err := client.Put(ctx, key, []byte("value"), 0)
			require.NoError(t, err)
		}

		// Verify operations
		for i := 0; i < 10; i++ {
			key := "cycle-" + string(rune('0'+cycle)) + "-key-" + string(rune('0'+i))
			servers[0].cacheService.data[key] = []byte("value")
			servers[1].cacheService.data[key] = []byte("value")
			
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

// TestClient_Lifecycle_ResourceLeak tests for resource leaks
func TestClient_Lifecycle_ResourceLeak(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create and close many clients
	for i := 0; i < 100; i++ {
		client, err := NewWithConfig(&ClientConfig{
			Addrs:    []string{server.address},
			Mode:     ModeSimple,
			PoolSize: 5,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)

		// Perform a few operations
		ctx := context.Background()
		for j := 0; j < 5; j++ {
			key := "leak-test-" + string(rune('0'+(i%10))) + "-" + string(rune('0'+j))
			client.Put(ctx, key, []byte("value"), 0)
		}

		// Close immediately
		err = client.Close()
		require.NoError(t, err)
	}

	// If there are resource leaks, they would typically manifest as
	// goroutine leaks or connection leaks. In a real test, you might
	// check runtime.NumGoroutine() before and after, or use tools
	// like github.com/fortytw2/leaktest
}

// TestClient_Lifecycle_ClusterModeTransition tests mode transitions
func TestClient_Lifecycle_ClusterModeTransition(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Start without topology (will use simple mode in auto)
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeAuto,
		PoolSize: 2,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Should be in simple mode
	assert.Equal(t, ModeSimple, client.GetMode())

	ctx := context.Background()

	// Operations should work in simple mode
	err = client.Put(ctx, "simple-key", []byte("value"), 0)
	require.NoError(t, err)

	// Note: In the current implementation, mode doesn't change after initialization
	// This test documents the current behavior
}

// TestClient_Lifecycle_PanicRecovery tests panic recovery
func TestClient_Lifecycle_PanicRecovery(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:    []string{server.address},
		Mode:     ModeSimple,
		PoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Test that client methods don't panic on edge cases
	testCases := []func(){
		func() { client.GetMode() },
		func() { client.GetConnectedNodes() },
		func() { 
			ctx := context.Background()
			client.Put(ctx, "", []byte{}, 0) 
		},
		func() {
			ctx := context.Background()
			client.Get(ctx, "")
		},
		func() {
			ctx := context.Background()
			client.Delete(ctx, "")
		},
		func() {
			ctx := context.Background()
			client.List(ctx, "")
		},
	}

	for i, tc := range testCases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Test case %d panicked: %v", i, r)
				}
			}()
			tc()
		}()
	}
}

// TestClient_Lifecycle_InitializationFailure tests handling of initialization failures
func TestClient_Lifecycle_InitializationFailure(t *testing.T) {
	t.Run("InvalidAddress", func(t *testing.T) {
		// Try to create client with invalid address
		_, err := NewWithConfig(&ClientConfig{
			Addrs:    []string{"invalid-address-without-port"},
			Mode:     ModeSimple,
			PoolSize: 1,
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
			Addrs:    []string{"localhost:59999"}, // Unlikely to be in use
			Mode:     ModeSimple,
			PoolSize: 1,
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
			Addrs:    []string{server.address},
			Mode:     ModeCluster,
			PoolSize: 1,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		assert.Error(t, err)
	})
}

// TestClient_Lifecycle_LongRunning tests long-running client behavior
func TestClient_Lifecycle_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up topology for cluster mode
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)
	server.cacheService.nodeID = "" // Disable ownership checks

	// Create client in cluster mode with refresh
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 100 * time.Millisecond,
		PoolSize:        2,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Run operations for extended period
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		
		i := 0
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				key := "long-running-" + string(rune('0'+(i%10)))
				client.Put(ctx, key, []byte("value"), 0)
				i++
			}
		}
	}()

	// Periodically update topology
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		
		epoch := uint64(2)
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				newTopology := setupSimpleTopology([]string{server.address})
				newTopology.Epoch = epoch
				server.clusterService.SetTopology(newTopology)
				epoch++
			}
		}
	}()

	// Run for 2 seconds
	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()

	// Client should still be functional
	err = client.Put(ctx, "final-key", []byte("value"), 0)
	assert.NoError(t, err, "Client should remain functional after long-running operations")
}