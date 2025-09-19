package cacheclient

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// connection wraps a single gRPC connection with health monitoring
type connection struct {
	address       string
	conn          *grpc.ClientConn
	client        pb.CacheServiceClient
	lastError     error
	lastErrorTime time.Time
	reconnecting  atomic.Bool
	mu            sync.RWMutex
}

// newConnection creates a new connection to the specified address
func newConnection(addr string, dialOpts []grpc.DialOption) (*connection, error) {
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %w", addr, err)
	}
	return &connection{
		address: addr,
		conn:    conn,
		client:  pb.NewCacheServiceClient(conn),
	}, nil
}

// close closes the connection
func (c *connection) close() error {
	return c.conn.Close()
}

// recordError records an error for health tracking
func (c *connection) recordError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.lastError = err
	c.lastErrorTime = time.Now()
	c.mu.Unlock()
}

// isHealthy checks if the connection is healthy
func (c *connection) isHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check gRPC connection state
	state := c.conn.GetState()
	if state == connectivity.TransientFailure || state == connectivity.Shutdown {
		return false
	}

	// Check if we've had recent errors
	if c.lastError != nil && time.Since(c.lastErrorTime) < ConnectionErrorWindow {
		// Only consider certain errors as unhealthy
		if isConnectionError(c.lastError) {
			return false
		}
	}

	return true
}

// reconnect attempts to re-establish the connection
func (c *connection) reconnect(dialOpts []grpc.DialOption) error {
	// Use atomic CAS to ensure only one goroutine reconnects at a time
	if !c.reconnecting.CompareAndSwap(false, true) {
		// Another goroutine is already reconnecting
		return nil
	}
	defer c.reconnecting.Store(false)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check again if connection is healthy while holding the lock
	state := c.conn.GetState()
	if state != connectivity.TransientFailure && state != connectivity.Shutdown {
		// Connection is fine, no need to reconnect
		return nil
	}

	// Close existing connection
	if c.conn != nil {
		c.conn.Close()
	}

	// Create new connection
	conn, err := grpc.Dial(c.address, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to reconnect to %s: %w", c.address, err)
	}

	c.conn = conn
	c.client = pb.NewCacheServiceClient(conn)
	c.lastError = nil
	c.lastErrorTime = time.Time{}

	return nil
}

// getClient returns the gRPC client for this connection
func (c *connection) getClient() pb.CacheServiceClient {
	return c.client
}
