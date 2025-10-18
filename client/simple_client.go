package cacheclient

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
)

// SimpleClient implements a simple round-robin cache client
type SimpleClient struct {
	*Operations               // Embedded for shared operations
	conns       []*connection // Array of connections
	addresses   []string      // List of addresses for consistent ordering
	currentIdx  atomic.Uint32 // Round-robin index
	config      *ClientConfig
	mu          sync.RWMutex
}

// NewSimpleClient creates a new SimpleClient with the given configuration
func NewSimpleClient(config *ClientConfig) (*SimpleClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.Addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}

	config.SetDefaults()

	client := &SimpleClient{
		config:    config,
		addresses: config.Addrs,
		conns:     make([]*connection, 0, len(config.Addrs)),
	}

	// Create connections for each address
	var lastErr error
	for _, addr := range client.addresses {
		conn, err := newConnection(addr, config.DialOpts, config.ConnectionPoolSize)
		if err != nil {
			lastErr = fmt.Errorf("failed to create connection for %s: %w", addr, err)
			// Continue trying other addresses
			continue
		}
		client.conns = append(client.conns, conn)
	}

	// Require at least one successful connection
	if len(client.conns) == 0 {
		if lastErr != nil {
			return nil, fmt.Errorf("failed to create any connections, last error: %w", lastErr)
		}
		return nil, fmt.Errorf("failed to create any connections")
	}

	// Initialize operations with this client as the router
	client.Operations = NewOperations(client)

	return client, nil
}

// Route selects a connection using hash-based routing for better key locality
// Implements Router interface
func (c *SimpleClient) Route(key string) (*connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.conns) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// Use hash-based routing for better key locality
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// Select connection based on hash
	idx := hash % uint32(len(c.conns))
	return c.conns[idx], nil
}

// RoundRobinRoute selects a connection using round-robin (for operations without keys)
// Implements Router interface
func (c *SimpleClient) RoundRobinRoute() (*connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.conns) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	idx := c.currentIdx.Add(1) - 1
	return c.conns[idx%uint32(len(c.conns))], nil
}

// The following methods are inherited from Operations:
// - Put
// - PutStream
// - Get
// - GetStream
// - GetRange
// - GetRangeStream
// - Delete
// - List
// - ListPage

// Close closes all connections
func (c *SimpleClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.conns = nil
	return firstErr
}

// GetMode returns the connection mode
func (c *SimpleClient) GetMode() ConnectionMode {
	return ModeSimple
}

// GetConnectedNodes returns the addresses of all connected nodes
func (c *SimpleClient) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.conns))
	for _, conn := range c.conns {
		nodes = append(nodes, conn.address)
	}
	sort.Strings(nodes)
	return nodes
}
