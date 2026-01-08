package cacheclient

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

// connection wraps multiple gRPC connections with health monitoring and load distribution
type connection struct {
	address       string
	connections   []*grpc.ClientConn
	clients       []pb.CacheServiceClient
	poolSize      int
	nextIndex     atomic.Uint64 // For round-robin selection
	lastError     error
	lastErrorTime time.Time
	reconnecting  atomic.Bool
	mu            sync.RWMutex

	// Epoch tracking for cluster mode
	epochGetter     EpochGetter
	onEpochMismatch EpochMismatchHandler
}

// newConnection creates a new connection pool to the specified address
func newConnection(addr string, dialOpts []grpc.DialOption, poolSize int) (*connection, error) {
	return newConnectionWithEpoch(addr, dialOpts, poolSize, nil, nil)
}

// newConnectionWithEpoch creates a new connection pool with epoch tracking support
func newConnectionWithEpoch(addr string, dialOpts []grpc.DialOption, poolSize int, epochGetter EpochGetter, onMismatch EpochMismatchHandler) (*connection, error) {
	if poolSize <= 0 {
		poolSize = 3 // Default pool size
	}

	c := &connection{
		address:         addr,
		poolSize:        poolSize,
		connections:     make([]*grpc.ClientConn, 0, poolSize),
		clients:         make([]pb.CacheServiceClient, 0, poolSize),
		epochGetter:     epochGetter,
		onEpochMismatch: onMismatch,
	}

	// Create multiple connections
	for i := 0; i < poolSize; i++ {
		opts := append([]grpc.DialOption{}, dialOpts...)
		opts = append(opts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
		))

		// Add epoch interceptors if epoch getter is provided (cluster mode)
		if epochGetter != nil {
			opts = append(opts,
				grpc.WithUnaryInterceptor(clientEpochUnaryInterceptor(epochGetter, onMismatch)),
				grpc.WithStreamInterceptor(clientEpochStreamInterceptor(epochGetter)),
			)
		}

		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			// Clean up any connections we've already created
			c.closeAll()
			return nil, fmt.Errorf("failed to create connection %d to %s: %w", i, addr, err)
		}

		c.connections = append(c.connections, conn)
		c.clients = append(c.clients, pb.NewCacheServiceClient(conn))
	}

	return c, nil
}

// close closes all connections
func (c *connection) close() error {
	return c.closeAll()
}

// closeAll closes all connections in the pool
func (c *connection) closeAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.connections {
		if conn != nil {
			if err := conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
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

// isHealthy checks if at least one connection in the pool is healthy
func (c *connection) isHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	healthyCount := 0
	for _, conn := range c.connections {
		if conn != nil {
			state := conn.GetState()
			if state == connectivity.Ready || state == connectivity.Idle {
				healthyCount++
			}
		}
	}

	// Consider healthy if at least one connection is healthy
	// Also check for recent errors if no connections are ready
	if healthyCount > 0 {
		return true
	}

	// If no connections are ready but we haven't had recent errors, still consider healthy
	// (connections might be in Connecting state)
	if c.lastError != nil && time.Since(c.lastErrorTime) < ConnectionErrorWindow {
		if isConnectionError(c.lastError) {
			return false
		}
	}

	return true
}

// reconnect attempts to re-establish unhealthy connections
func (c *connection) reconnect(dialOpts []grpc.DialOption) error {
	// Use atomic CAS to ensure only one goroutine reconnects at a time
	if !c.reconnecting.CompareAndSwap(false, true) {
		// Another goroutine is already reconnecting
		return nil
	}
	defer c.reconnecting.Store(false)

	return c.reconnectUnhealthy(dialOpts)
}

// reconnectUnhealthy reconnects only the unhealthy connections in the pool
func (c *connection) reconnectUnhealthy(dialOpts []grpc.DialOption) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var reconnectErr error
	for i, conn := range c.connections {
		if conn == nil {
			continue
		}

		state := conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			// Close the unhealthy connection
			conn.Close()

			opts := append([]grpc.DialOption{}, dialOpts...)
			opts = append(opts, grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageSize),
			))

			// Create new connection
			newConn, err := grpc.Dial(c.address, opts...)
			if err != nil {
				// Continue trying to reconnect other connections
				if reconnectErr == nil {
					reconnectErr = fmt.Errorf("failed to reconnect connection %d to %s: %w", i, c.address, err)
				}
				// Set to nil to mark as failed
				c.connections[i] = nil
				c.clients[i] = nil
				continue
			}

			c.connections[i] = newConn
			c.clients[i] = pb.NewCacheServiceClient(newConn)
		}
	}

	// Clear error if we successfully reconnected at least one connection
	if reconnectErr == nil {
		c.lastError = nil
		c.lastErrorTime = time.Time{}
	}

	return reconnectErr
}

// getClient returns a gRPC client using round-robin selection
// Multiple goroutines can use the returned client concurrently
func (c *connection) getClient() pb.CacheServiceClient {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.clients) == 0 {
		return nil
	}

	// Try to find a healthy client using round-robin
	startIndex := c.nextIndex.Add(1)
	for i := 0; i < len(c.clients); i++ {
		index := (startIndex + uint64(i)) % uint64(len(c.clients))
		client := c.clients[index]

		// Check if this client's connection is healthy
		if client != nil && c.connections[index] != nil {
			state := c.connections[index].GetState()
			if state != connectivity.TransientFailure && state != connectivity.Shutdown {
				return client
			}
		}
	}

	// If no healthy clients found, return the first non-nil client
	// (it might recover or we might get a better error message)
	for _, client := range c.clients {
		if client != nil {
			return client
		}
	}

	return nil
}

// getHealthStats returns health statistics for monitoring
func (c *connection) getHealthStats() (healthy, total int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total = len(c.connections)
	for _, conn := range c.connections {
		if conn != nil {
			state := conn.GetState()
			if state == connectivity.Ready || state == connectivity.Idle {
				healthy++
			}
		}
	}
	return healthy, total
}

// EpochGetter is a function that returns the current client epoch
type EpochGetter func() uint64

// EpochMismatchHandler is called when server epoch differs from client epoch
type EpochMismatchHandler func(clientEpoch, serverEpoch uint64)

// clientEpochUnaryInterceptor creates a unary client interceptor that:
// 1. Attaches client epoch to outgoing requests
// 2. Detects server epoch from responses (for cache invalidation)
func clientEpochUnaryInterceptor(epochGetter EpochGetter, onMismatch EpochMismatchHandler) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Attach client epoch to outgoing metadata
		clientEpoch := epochGetter()
		ctx = metadata.AppendToOutgoingContext(ctx,
			coordinator.MetadataKeyRingEpoch, strconv.FormatUint(clientEpoch, 10),
		)

		// Capture response headers
		var header metadata.MD
		opts = append(opts, grpc.Header(&header))

		// Make the call
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Check for epoch mismatch in response
		if serverEpochs := header.Get(coordinator.MetadataKeyRingEpoch); len(serverEpochs) > 0 {
			if serverEpoch, parseErr := strconv.ParseUint(serverEpochs[0], 10, 64); parseErr == nil {
				if serverEpoch != clientEpoch && onMismatch != nil {
					onMismatch(clientEpoch, serverEpoch)
				}
			}
		}

		return err
	}
}

// clientEpochStreamInterceptor creates a stream client interceptor that attaches epoch
func clientEpochStreamInterceptor(epochGetter EpochGetter) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Attach client epoch to outgoing metadata
		clientEpoch := epochGetter()
		ctx = metadata.AppendToOutgoingContext(ctx,
			coordinator.MetadataKeyRingEpoch, strconv.FormatUint(clientEpoch, 10),
		)

		return streamer(ctx, desc, cc, method, opts...)
	}
}
