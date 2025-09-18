package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestConcurrent_MixedOperations tests concurrent CRUD operations
func TestConcurrent_MixedOperations(t *testing.T) {
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
	var wg sync.WaitGroup

	// Counters for operations
	puts := int32(0)
	gets := int32(0)
	deletes := int32(0)
	lists := int32(0)
	errors := int32(0)

	// Concurrent Put operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("put-key-%d-%d", id, j)
				err := client.Put(ctx, key, []byte("value"), 0)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&puts, 1)
				}
			}
		}(i)
	}

	// Concurrent Get operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("put-key-%d-%d", id, j)
				// Small delay to allow puts to complete
				time.Sleep(10 * time.Millisecond)
				_, err := client.Get(ctx, key)
				if err != nil {
					// Some gets may fail if key doesn't exist yet
					continue
				}
				atomic.AddInt32(&gets, 1)
			}
		}(i)
	}

	// Concurrent Delete operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("delete-key-%d-%d", id, j)
				// First put, then delete
				client.Put(ctx, key, []byte("temp"), 0)
				err := client.Delete(ctx, key)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&deletes, 1)
				}
			}
		}(i)
	}

	// Concurrent List operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := client.List(ctx, "put-key-")
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&lists, 1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	// Wait for all operations
	wg.Wait()

	// Verify operations completed
	t.Logf("Operations - Puts: %d, Gets: %d, Deletes: %d, Lists: %d, Errors: %d",
		puts, gets, deletes, lists, errors)

	assert.Equal(t, int32(100), puts, "All puts should complete")
	assert.Greater(t, gets, int32(50), "Most gets should succeed")
	assert.Equal(t, int32(50), deletes, "All deletes should complete")
	assert.Equal(t, int32(50), lists, "All lists should complete")
	assert.Less(t, errors, int32(10), "Errors should be minimal")
}

// TestConcurrent_TopologyUpdates tests operations during topology changes
func TestConcurrent_TopologyUpdates(t *testing.T) {
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

	// Initial topology
	topology := setupSimpleTopology(addresses)
	servers[0].clusterService.SetTopology(topology)

	// Create client in cluster mode
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{servers[0].address},
		Mode:            ModeCluster,
		RefreshInterval: 50 * time.Millisecond, // Fast refresh for testing
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Disable ownership checks
	for _, server := range servers {
		server.cacheService.nodeID = ""
	}

	ctx := context.Background()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Counter for successful operations
	successfulOps := int32(0)
	topologyChanges := int32(0)

	// Goroutine to continuously change topology
	wg.Add(1)
	go func() {
		defer wg.Done()
		epoch := uint64(2)
		for {
			select {
			case <-stopCh:
				return
			default:
				// Rotate which nodes are active
				newTopology := setupSimpleTopology(addresses)
				newTopology.Epoch = epoch

				// Randomly mark a node as down
				if epoch%3 == 0 && len(newTopology.Nodes) > 1 {
					newTopology.Nodes[int(epoch)%len(newTopology.Nodes)].Status = 2 // DOWN
				}

				servers[0].clusterService.SetTopology(newTopology)
				atomic.AddInt32(&topologyChanges, 1)
				epoch++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Concurrent operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("topology-key-%d", id)
					err := client.Put(ctx, key, []byte("value"), 0)
					if err == nil {
						atomic.AddInt32(&successfulOps, 1)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("Topology changes: %d, Successful operations: %d", topologyChanges, successfulOps)
	assert.Greater(t, topologyChanges, int32(10), "Should have multiple topology changes")
	assert.Greater(t, successfulOps, int32(50), "Operations should continue during topology changes")
}

// TestConcurrent_PoolAccess tests pool thread safety
func TestConcurrent_PoolAccess(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Create client with small pool to stress contention
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
	concurrency := 100
	opsPerWorker := 100
	var wg sync.WaitGroup

	// Track successful operations
	successful := int32(0)

	// Pre-populate some test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("pool-key-%d", i)
		server.cacheService.data[key] = []byte("initial-value")
	}

	// Launch many concurrent workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("pool-key-%d", j%10)
				// Mix of operations
				switch j % 3 {
				case 0:
					err := client.Put(ctx, key, []byte("value"), 0)
					if err == nil {
						atomic.AddInt32(&successful, 1)
					}
				case 1:
					_, err := client.Get(ctx, key)
					if err == nil {
						atomic.AddInt32(&successful, 1)
					}
				case 2:
					client.Delete(ctx, key)
					atomic.AddInt32(&successful, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedOps := int32(concurrency * opsPerWorker)
	t.Logf("Successful operations: %d/%d", successful, expectedOps)
	assert.Greater(t, successful, expectedOps*9/10, "Most operations should succeed")
}

// TestConcurrent_RaceConditions uses Go race detector
func TestConcurrent_RaceConditions(t *testing.T) {
	// This test is specifically for running with -race flag
	// It performs various concurrent operations to detect races

	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Set up cluster topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client in cluster mode (more complex, more potential for races)
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 10 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	server.cacheService.nodeID = "" // Disable ownership checks

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent operations that might trigger races
	operations := []func(){
		func() { client.Put(ctx, "race-key", []byte("value"), 0) },
		func() { client.Get(ctx, "race-key") },
		func() { client.Delete(ctx, "race-key") },
		func() { client.List(ctx, "") },
		func() { client.GetMode() },
		func() { client.GetConnectedNodes() },
		func() {
			// Trigger topology update
			newTopo := setupSimpleTopology([]string{server.address})
			newTopo.Epoch = 10
			server.clusterService.SetTopology(newTopo)
		},
	}

	// Run operations concurrently
	for i := 0; i < 50; i++ {
		for _, op := range operations {
			wg.Add(1)
			operation := op
			go func() {
				defer wg.Done()
				operation()
			}()
		}
	}

	wg.Wait()
}

// TestConcurrent_StreamingOperations tests concurrent streaming
func TestConcurrent_StreamingOperations(t *testing.T) {
	// Create server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Prepare test data
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

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
	var wg sync.WaitGroup
	errors := int32(0)
	successes := int32(0)

	// Store large data for streaming
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("stream-key-%d", i)
		server.cacheService.data[key] = largeData
	}

	// Concurrent streaming reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("stream-key-%d", j)
				buf := &safeBuffer{}
				err := client.GetStream(ctx, key, buf)
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					if buf.Len() == len(largeData) {
						atomic.AddInt32(&successes, 1)
					}
				}
			}
		}(i)
	}

	// Concurrent streaming writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("write-stream-%d", id)
			reader := &safeReader{data: largeData}
			err := client.PutStream(ctx, key, reader, 0)
			if err != nil {
				atomic.AddInt32(&errors, 1)
			} else {
				atomic.AddInt32(&successes, 1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Streaming operations - Successes: %d, Errors: %d", successes, errors)
	assert.Greater(t, successes, int32(50), "Most streaming operations should succeed")
	assert.Less(t, errors, int32(10), "Errors should be minimal")
}

// TestConcurrent_LoadBalancing verifies load distribution
func TestConcurrent_LoadBalancing(t *testing.T) {
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
		Addrs: addresses,
		Mode:  ModeSimple,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Launch concurrent operations
	numWorkers := 30
	opsPerWorker := 100

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("lb-key-%d", (id*opsPerWorker+j)%26)
				client.Put(ctx, key, []byte("value"), 0)
			}
		}(i)
	}

	wg.Wait()

	// Check load distribution
	totalOps := 0
	for i, server := range servers {
		putCount, _, _, _ := server.GetCallCounts()
		t.Logf("Server %d received %d requests", i, putCount)
		totalOps += int(putCount)

		// Each server should get a reasonable share
		expectedShare := numWorkers * opsPerWorker / len(servers)
		minShare := expectedShare / 2
		maxShare := expectedShare * 2
		assert.Greater(t, int(putCount), minShare, "Server %d should receive at least %d requests", i, minShare)
		assert.Less(t, int(putCount), maxShare, "Server %d should receive at most %d requests", i, maxShare)
	}

	assert.Equal(t, numWorkers*opsPerWorker, totalOps, "All operations should be accounted for")
}

// safeBuffer is a thread-safe buffer for testing
type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *safeBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}

// safeReader is a thread-safe reader for testing
type safeReader struct {
	mu   sync.Mutex
	data []byte
	pos  int
}

func (s *safeReader) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.data) {
		return 0, io.EOF
	}
	n = copy(p, s.data[s.pos:])
	s.pos += n
	return n, nil
}
