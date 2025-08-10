package cacheclient

import (
	"context"
	"fmt"
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

// Get returns a client from the pool using round-robin selection.
func (p *ConnectionPool) Get() *Client {
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

// Execute runs a function with a client from the pool.
func (p *ConnectionPool) Execute(ctx context.Context, fn func(context.Context, *Client) error) error {
	client := p.Get()
	return fn(ctx, client)
}
