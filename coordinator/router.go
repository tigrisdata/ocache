package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// RouterConfig contains configuration for the Router
type RouterConfig struct {
	// Connection timeout for establishing new connections
	ConnectionTimeout time.Duration
	// Maximum message size for sending (in bytes)
	MaxSendMsgSize int
	// Maximum message size for receiving (in bytes)
	MaxRecvMsgSize int
	// Number of retry attempts for transient failures
	MaxRetries int
	// Initial retry backoff duration
	InitialRetryBackoff time.Duration
	// Maximum retry backoff duration
	MaxRetryBackoff time.Duration
	// Keepalive parameters
	KeepaliveTime    time.Duration // Send keepalive ping every this duration
	KeepaliveTimeout time.Duration // Wait this long for keepalive response
	// Circuit breaker parameters
	CircuitBreakerThreshold int           // Number of consecutive failures to open circuit
	CircuitBreakerTimeout   time.Duration // How long to wait before attempting to close circuit
}

// DefaultRouterConfig returns a RouterConfig with sensible defaults
func DefaultRouterConfig() *RouterConfig {
	return &RouterConfig{
		ConnectionTimeout:       5 * time.Second,
		MaxSendMsgSize:          MaxMessageSize, // 128MB
		MaxRecvMsgSize:          MaxMessageSize, // 128MB
		MaxRetries:              3,
		InitialRetryBackoff:     100 * time.Millisecond,
		MaxRetryBackoff:         5 * time.Second,
		KeepaliveTime:           30 * time.Second,
		KeepaliveTimeout:        10 * time.Second,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
	}
}

// clientState tracks the state of a client connection
type clientState struct {
	client          pb.CacheServiceClient
	conn            *grpc.ClientConn
	failureCount    int32
	circuitOpenTime time.Time
	circuitOpen     int32 // atomic: 0=closed, 1=open
	lastFailure     time.Time
	mu              sync.RWMutex
}

// ConnectionStats represents statistics for a single connection
type ConnectionStats struct {
	State           string
	FailureCount    int32
	CircuitOpen     bool
	LastFailure     time.Time
	CircuitOpenTime time.Time
}

// Router is a router for routing requests to the appropriate node
type Router struct {
	ring    *Ring
	clients map[string]*clientState
	localID string
	config  *RouterConfig
	mu      sync.RWMutex
}

// NewRouter creates a new router with the default configuration
func NewRouter(ring *Ring, localID string) *Router {
	return NewRouterWithConfig(ring, localID, DefaultRouterConfig())
}

// NewRouterWithConfig creates a new router with a custom configuration
func NewRouterWithConfig(ring *Ring, localID string, config *RouterConfig) *Router {
	if config == nil {
		config = DefaultRouterConfig()
	}
	return &Router{
		ring:    ring,
		clients: make(map[string]*clientState),
		localID: localID,
		config:  config,
	}
}

// Route returns a client for routing requests for the given key
// Returns an error if the key should be handled locally (defensive check)
func (r *Router) Route(key string) (pb.CacheServiceClient, error) {
	return r.RouteWithRetry(key, r.config.MaxRetries)
}

// RouteWithRetry returns a client for routing with configurable retry attempts
// Returns an error if the key maps to the local node (this should not happen
// as callers should check IsLocal first, but we check defensively)
func (r *Router) RouteWithRetry(key string, maxRetries int) (pb.CacheServiceClient, error) {
	node, err := r.ring.GetNode(key)
	if err != nil {
		return nil, err
	}

	// Defensive check: callers should use IsLocal() before calling Route()
	// If we get here with a local key, it's likely a bug in the caller
	if node.ID == r.localID {
		zlog.Warn().
			Str("key", key).
			Str("node_id", node.ID).
			Msg("Received request to route to local node")

		return nil, NewLocalRoutingError(r.localID, key)
	}

	var lastErr error
	backoff := r.config.InitialRetryBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry with exponential backoff
			time.Sleep(backoff)
			backoff = r.calculateBackoff(backoff)

			zlog.Debug().
				Str("node_id", node.ID).
				Int("attempt", attempt).
				Dur("backoff", backoff).
				Msg("Retrying connection after failure")
		}

		client, err := r.getClient(node.ID)
		if err == nil {
			zlog.Debug().
				Str("node_id", node.ID).
				Msg("Successfully routed to node")

			return client, nil
		}

		lastErr = err

		// Don't retry if it's not a retryable error
		if !IsRetryableError(err) {
			break
		}
	}

	return nil, NewMaxRetriesExceededError(node.ID, key, maxRetries+1, lastErr)
}

// getClient returns a client for the given node, creating one if necessary
func (r *Router) getClient(nodeID string) (pb.CacheServiceClient, error) {
	// Fast path: check if client exists and is healthy
	r.mu.RLock()
	state, exists := r.clients[nodeID]
	r.mu.RUnlock()

	zlog.Debug().
		Str("node_id", nodeID).
		Msg("Checking if client exists and is healthy")

	if exists && state != nil {
		// Check if circuit breaker is open
		if r.isCircuitOpen(state) {
			zlog.Warn().
				Str("node_id", nodeID).
				Msg("Circuit breaker open for node")

			return nil, NewCircuitBreakerOpenError(nodeID)
		}

		// Check connection state
		if state.conn != nil {
			connState := state.conn.GetState()
			if connState != connectivity.Shutdown && connState != connectivity.TransientFailure {
				return state.client, nil
			}
		}
	}

	// Slow path: create new client or reconnect
	r.mu.Lock()
	defer r.mu.Unlock()

	zlog.Debug().
		Str("node_id", nodeID).
		Msg("Creating new client or reconnecting")

	// Double-check after acquiring write lock
	state, exists = r.clients[nodeID]
	if exists && state != nil {
		if r.isCircuitOpen(state) {
			zlog.Warn().
				Str("node_id", nodeID).
				Msg("Circuit breaker open for node")

			return nil, NewCircuitBreakerOpenError(nodeID)
		}

		if state.conn != nil {
			connState := state.conn.GetState()
			if connState != connectivity.Shutdown && connState != connectivity.TransientFailure {
				return state.client, nil
			}
		}
	}

	// Get node listen address from ring
	nodes := r.ring.GetAllNodes()
	var nodeAddr string
	for _, node := range nodes {
		if node.ID == nodeID {
			// Use listen address for client connections (not cluster address)
			nodeAddr = node.ListenAddress
			if nodeAddr == "" {
				// Fallback to cluster address for backward compatibility
				nodeAddr = node.Address
			}
			break
		}
	}

	if nodeAddr == "" {
		zlog.Warn().
			Str("node_id", nodeID).
			Msg("Node not found in ring")

		return nil, NewNodeNotFoundError(nodeID, "")
	}

	// Create or update client state
	if state == nil {
		state = &clientState{}
		r.clients[nodeID] = state
	}

	// Close existing connection if any
	if state.conn != nil {
		state.conn.Close()
	}

	// Create new connection with keepalive and without blocking
	conn, err := r.createConnection(nodeAddr)
	if err != nil {
		r.recordFailureAndOpenCircuit(state)

		zlog.Warn().
			Str("node_id", nodeID).
			Str("address", nodeAddr).
			Msg("Failed to create connection to node")

		return nil, NewConnectionFailedError(nodeID, nodeAddr, err)
	}

	client := pb.NewCacheServiceClient(conn)
	state.client = client
	state.conn = conn

	// Check connection state after brief delay to allow state to settle
	// Since we use non-blocking dial, we need to verify the connection is actually healthy
	time.Sleep(10 * time.Millisecond)
	connState := conn.GetState()
	if connState == connectivity.TransientFailure || connState == connectivity.Shutdown {
		r.recordFailureAndOpenCircuit(state)

		zlog.Warn().
			Str("node_id", nodeID).
			Str("address", nodeAddr).
			Str("state", connState.String()).
			Msg("Connection is in failed state")

		return nil, NewConnectionFailedError(nodeID, nodeAddr,
			fmt.Errorf("connection in failed state: %s", connState))
	}

	atomic.StoreInt32(&state.failureCount, 0) // Reset failure count on successful connection

	zlog.Debug().
		Str("node_id", nodeID).
		Str("address", nodeAddr).
		Msg("Created connection to node")

	return client, nil
}

// createConnection creates a new gRPC connection with configured parameters
func (r *Router) createConnection(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ConnectionTimeout)
	defer cancel()

	zlog.Debug().
		Str("address", address).
		Msg("Creating connection")

	// Configure keepalive parameters
	keepaliveParams := keepalive.ClientParameters{
		Time:                r.config.KeepaliveTime,
		Timeout:             r.config.KeepaliveTimeout,
		PermitWithoutStream: true,
	}

	// Create connection without blocking
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(r.config.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(r.config.MaxSendMsgSize),
		),
		// Remove WithBlock() - connection will be established lazily
	)

	zlog.Debug().
		Str("address", address).
		Msg("Created connection")

	return conn, err
}

// isCircuitOpen checks if the circuit breaker is open for a client
func (r *Router) isCircuitOpen(state *clientState) bool {
	if atomic.LoadInt32(&state.circuitOpen) == 0 {
		return false
	}

	// Check if circuit breaker timeout has expired
	state.mu.RLock()
	openTime := state.circuitOpenTime
	state.mu.RUnlock()

	if time.Since(openTime) > r.config.CircuitBreakerTimeout {
		// Attempt to close circuit using compare-and-swap
		if atomic.CompareAndSwapInt32(&state.circuitOpen, 1, 0) {
			// Only reset failure count if we successfully closed the circuit
			atomic.StoreInt32(&state.failureCount, 0)
		}
		return false
	}

	return true
}

// recordFailureAndOpenCircuit records a failure and potentially opens the circuit breaker
func (r *Router) recordFailureAndOpenCircuit(state *clientState) {
	failures := atomic.AddInt32(&state.failureCount, 1)

	state.mu.Lock()
	state.lastFailure = time.Now()
	state.mu.Unlock()

	if failures >= int32(r.config.CircuitBreakerThreshold) {
		if atomic.CompareAndSwapInt32(&state.circuitOpen, 0, 1) {
			state.mu.Lock()
			state.circuitOpenTime = time.Now()
			state.mu.Unlock()

			zlog.Warn().
				Int32("failure_count", failures).
				Msg("Circuit breaker opened due to consecutive failures")
		}
	}
}

// calculateBackoff calculates the next backoff duration with exponential increase
func (r *Router) calculateBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * 2)
	if next > r.config.MaxRetryBackoff {
		return r.config.MaxRetryBackoff
	}
	return next
}

func (r *Router) IsLocal(key string) bool {
	return r.ring.IsLocal(key)
}

// RemoveClient removes and closes the client connection for a node
func (r *Router) RemoveClient(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if state, exists := r.clients[nodeID]; exists {
		if state.conn != nil {
			state.conn.Close()
		}
		delete(r.clients, nodeID)

		zlog.Debug().
			Str("node_id", nodeID).
			Msg("Removed client connection")
	}
}

// Close closes all client connections
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for nodeID, state := range r.clients {
		if state != nil && state.conn != nil {
			if err := state.conn.Close(); err != nil {
				zlog.Error().
					Err(err).
					Str("node_id", nodeID).
					Msg("Error closing connection")
			}
		}
	}

	r.clients = make(map[string]*clientState)

	zlog.Debug().
		Msg("Closed all client connections")

	return nil
}

// RefreshConnections removes connections to inactive nodes
func (r *Router) RefreshConnections() {
	r.mu.Lock()
	defer r.mu.Unlock()

	zlog.Debug().
		Msg("Refreshing connections")

	// Get active nodes
	activeNodes := r.ring.GetActiveNodes()
	activeNodeMap := make(map[string]bool)
	for _, node := range activeNodes {
		activeNodeMap[node.ID] = true
	}

	// Remove connections to inactive nodes
	for nodeID, state := range r.clients {
		if !activeNodeMap[nodeID] {
			if state != nil && state.conn != nil {
				state.conn.Close()
			}
			delete(r.clients, nodeID)

		}
	}
}

// GetConnectionStats returns statistics about current connections
func (r *Router) GetConnectionStats() map[string]ConnectionStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make(map[string]ConnectionStats)
	for nodeID, state := range r.clients {
		if state == nil {
			continue
		}

		var connState connectivity.State
		if state.conn != nil {
			connState = state.conn.GetState()
		}

		state.mu.RLock()
		lastFailure := state.lastFailure
		circuitOpenTime := state.circuitOpenTime
		state.mu.RUnlock()

		stats[nodeID] = ConnectionStats{
			State:           connState.String(),
			FailureCount:    atomic.LoadInt32(&state.failureCount),
			CircuitOpen:     atomic.LoadInt32(&state.circuitOpen) == 1,
			LastFailure:     lastFailure,
			CircuitOpenTime: circuitOpenTime,
		}
	}

	return stats
}
