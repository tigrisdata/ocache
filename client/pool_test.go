package cacheclient

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnectionPool(t *testing.T) {
	t.Run("valid pool creation", func(t *testing.T) {
		// Skip this test as it would require an actual server
		// The pool creation is tested indirectly through other tests
		t.Skip("Skipping actual connection test - requires server")
	})

	t.Run("invalid pool size", func(t *testing.T) {
		pool, err := NewConnectionPool("localhost:9999", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool size must be positive")
		assert.Nil(t, pool)

		pool, err = NewConnectionPool("localhost:9999", -1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool size must be positive")
		assert.Nil(t, pool)
	})
}

// MockPoolForTesting creates a pool with mock clients for testing
func mockPoolForTesting(size int) *ConnectionPool {
	pool := &ConnectionPool{
		clients: make([]*Client, size),
		size:    size,
	}

	for i := 0; i < size; i++ {
		pool.clients[i] = &Client{
			client: &mockCacheServiceClient{},
		}
	}

	return pool
}

func TestConnectionPool_GetClient(t *testing.T) {
	pool := mockPoolForTesting(3)

	// Test round-robin distribution
	client1 := pool.GetClient()
	client2 := pool.GetClient()
	client3 := pool.GetClient()
	client4 := pool.GetClient() // Should wrap around to first client

	assert.NotNil(t, client1)
	assert.NotNil(t, client2)
	assert.NotNil(t, client3)
	assert.NotNil(t, client4)
	assert.Same(t, client1, client4) // Round-robin should wrap
}

func TestConnectionPool_GetAll(t *testing.T) {
	pool := mockPoolForTesting(3)

	clients := pool.GetAll()
	assert.Len(t, clients, 3)
	for _, client := range clients {
		assert.NotNil(t, client)
	}
}

func TestConnectionPool_Put(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	// Set up mock to track calls
	for _, client := range pool.clients {
		mock := &mockCacheServiceClient{}
		client.client = mock
	}

	err := pool.Put(ctx, "key1", []byte("value1"), 60)
	assert.NoError(t, err)

	// Verify that one of the clients was used
	mock := pool.clients[0].client.(*mockCacheServiceClient)
	assert.True(t, mock.putObjectCalled)
}

func TestConnectionPool_PutStream(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	data := bytes.NewReader([]byte("stream data"))
	err := pool.PutStream(ctx, "key1", data, 60)
	assert.NoError(t, err)

	// Verify that streaming was used
	mock := pool.clients[0].client.(*mockCacheServiceClient)
	assert.Greater(t, len(mock.putStreamData), 0)
}

func TestConnectionPool_GetData(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	// Set up mock data
	for _, client := range pool.clients {
		mock := &mockCacheServiceClient{
			getData: [][]byte{[]byte("test"), []byte("data")},
		}
		client.client = mock
	}

	data, err := pool.Get(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("testdata"), data)
}

func TestConnectionPool_GetStream(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	// Set up mock data
	for _, client := range pool.clients {
		mock := &mockCacheServiceClient{
			getData: [][]byte{[]byte("stream"), []byte("data")},
		}
		client.client = mock
	}

	var buf bytes.Buffer
	err := pool.GetStream(ctx, "key1", &buf)
	assert.NoError(t, err)
	assert.Equal(t, "streamdata", buf.String())
}

func TestConnectionPool_Delete(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	err := pool.Delete(ctx, "key1")
	assert.NoError(t, err)

	// Verify delete was called
	mock := pool.clients[0].client.(*mockCacheServiceClient)
	assert.True(t, mock.deleteCalled)
}

func TestConnectionPool_List(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	// Set up mock data
	for _, client := range pool.clients {
		mock := &mockCacheServiceClient{
			listKeys: []string{"key1", "key2", "key3"},
		}
		client.client = mock
	}

	keys, err := pool.List(ctx, "")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"key1", "key2", "key3"}, keys)
}

func TestConnectionPool_Execute_BackwardCompatibility(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(2)

	// Test that Execute still works for backward compatibility
	executed := false
	err := pool.Execute(ctx, func(ctx context.Context, client *Client) error {
		executed = true
		assert.NotNil(t, client)
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, executed)
}

func TestConnectionPool_ConcurrentAccess(t *testing.T) {
	ctx := context.TODO()
	pool := mockPoolForTesting(3)

	// Set up mock data for all clients
	for _, client := range pool.clients {
		mock := &mockCacheServiceClient{
			getData: [][]byte{[]byte("concurrent")},
		}
		client.client = mock
	}

	// Run multiple goroutines accessing the pool concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Mix of different operations
			switch id % 4 {
			case 0:
				if err := pool.Put(ctx, "key", []byte("value"), 0); err != nil {
					errors <- err
				}
			case 1:
				if _, err := pool.Get(ctx, "key"); err != nil {
					errors <- err
				}
			case 2:
				if err := pool.Delete(ctx, "key"); err != nil {
					errors <- err
				}
			case 3:
				if _, err := pool.List(ctx, ""); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		assert.NoError(t, err)
	}
}

func TestConnectionPool_RoundRobinDistribution(t *testing.T) {
	pool := mockPoolForTesting(3)

	// Track which clients are used
	usedClients := make(map[*Client]int)

	// Make 9 requests (3 full rounds)
	for i := 0; i < 9; i++ {
		client := pool.GetClient()
		usedClients[client]++
	}

	// Each client should have been used exactly 3 times
	assert.Len(t, usedClients, 3)
	for _, count := range usedClients {
		assert.Equal(t, 3, count)
	}
}

// ExampleConnectionPool demonstrates how to use the improved pool interface
func ExampleConnectionPool() {
	// Create a connection pool with 5 connections
	pool, err := NewConnectionPool("localhost:9000", 5)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Use the pool just like a single client
	// The pool automatically distributes requests across connections

	// Store a value
	err = pool.Put(ctx, "mykey", []byte("myvalue"), 3600)
	if err != nil {
		panic(err)
	}

	// Retrieve a value
	data, err := pool.Get(ctx, "mykey")
	if err != nil {
		panic(err)
	}
	_ = data

	// Stream large data
	largeData := strings.NewReader("very large data...")
	err = pool.PutStream(ctx, "largekey", largeData, 3600)
	if err != nil {
		panic(err)
	}

	// List keys with prefix
	keys, err := pool.List(ctx, "my")
	if err != nil {
		panic(err)
	}
	_ = keys

	// Delete a key
	err = pool.Delete(ctx, "mykey")
	if err != nil {
		panic(err)
	}
}

func TestConnectionPool_InterfaceCompatibility(t *testing.T) {
	// This test ensures that ConnectionPool can be used wherever
	// the same interface as Client is expected

	type CacheOperations interface {
		Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error
		PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error
		Get(ctx context.Context, key string) ([]byte, error)
		GetStream(ctx context.Context, key string, w io.Writer) error
		Delete(ctx context.Context, key string) error
		List(ctx context.Context, prefix string) ([]string, error)
	}

	// Both Client and ConnectionPool should implement the same interface
	var _ CacheOperations = (*Client)(nil)
	var _ CacheOperations = (*ConnectionPool)(nil)

	// This means you can use pool wherever you use client
	var cache CacheOperations

	// Can use a single client
	cache = &Client{client: &mockCacheServiceClient{}}
	require.NotNil(t, cache)

	// Or can use a pool - it's a drop-in replacement
	cache = mockPoolForTesting(3)
	require.NotNil(t, cache)

	// The interface is identical
	ctx := context.TODO()
	err := cache.Put(ctx, "key", []byte("value"), 0)
	assert.NoError(t, err)
}
