package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
)

// ConnectionPool manages a pool of gRPC client connections.
type ConnectionPool struct {
	clients []*Client
	size    int
	mu      sync.Mutex
	index   int
}

// NewConnectionPool creates a new connection pool with the specified size.
func NewConnectionPool(addr string, size int, opts ...grpc.DialOption) (*ConnectionPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("pool size must be positive")
	}

	pool := &ConnectionPool{
		clients: make([]*Client, size),
		size:    size,
	}

	// Create all connections
	for i := 0; i < size; i++ {
		client, err := New(addr, opts...)
		if err != nil {
			// Clean up already created connections
			for j := 0; j < i; j++ {
				pool.clients[j].Close()
			}
			return nil, fmt.Errorf("failed to create connection %d: %w", i, err)
		}
		pool.clients[i] = client
	}

	return pool, nil
}

// GetClient returns a client from the pool using round-robin selection.
// This method is exposed for advanced use cases where you need direct access to a client.
// For normal operations, use the direct methods (Put, Get, Delete, List) instead.
func (p *ConnectionPool) GetClient() *Client {
	p.mu.Lock()
	client := p.clients[p.index]
	p.index = (p.index + 1) % p.size
	p.mu.Unlock()
	return client
}

// GetAll returns all clients in the pool.
// Useful when you want to distribute clients across workers.
func (p *ConnectionPool) GetAll() []*Client {
	return p.clients
}

// Close closes all connections in the pool.
func (p *ConnectionPool) Close() error {
	var firstErr error
	for _, client := range p.clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Put stores a key-value pair in the cache using a client from the pool.
func (p *ConnectionPool) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	client := p.GetClient()
	return client.Put(ctx, key, data, ttlSeconds)
}

// PutStream streams data from an io.Reader to the cache service using a client from the pool.
// This is efficient for large values.
func (p *ConnectionPool) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	client := p.GetClient()
	return client.PutStream(ctx, key, r, ttlSeconds)
}

// Get retrieves a value from the cache using a client from the pool.
func (p *ConnectionPool) Get(ctx context.Context, key string) ([]byte, error) {
	client := p.GetClient()
	return client.Get(ctx, key)
}

// GetStream streams a value directly to the provided writer using a client from the pool.
// This is efficient for large values.
func (p *ConnectionPool) GetStream(ctx context.Context, key string, w io.Writer) error {
	client := p.GetClient()
	return client.GetStream(ctx, key, w)
}

// Delete removes a key from the cache using a client from the pool.
func (p *ConnectionPool) Delete(ctx context.Context, key string) error {
	client := p.GetClient()
	return client.Delete(ctx, key)
}

// List returns all keys matching the given prefix using a client from the pool.
func (p *ConnectionPool) List(ctx context.Context, prefix string) ([]string, error) {
	client := p.GetClient()
	return client.List(ctx, prefix)
}
