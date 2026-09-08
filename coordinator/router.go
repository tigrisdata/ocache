// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Ring defines the interface for the hash ring used by the router.
// This interface allows for testing with mock implementations.
type Ring interface {
	// GetNode returns the node that owns the given key
	GetNode(key string) (*ring.NodeInfo, error)
	// GetAllNodes returns all nodes in the ring
	GetAllNodes() []*ring.NodeInfo
	// GetActiveNodes returns all active nodes in the ring
	GetActiveNodes() []*ring.NodeInfo
	// IsLocal returns true if the local node owns the given key
	IsLocal(key string) bool
}

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
	// GRPCDialOptions are additional gRPC dial options applied to all outgoing connections.
	// These are appended after the default options (transport credentials, keepalive, message size).
	GRPCDialOptions []grpc.DialOption
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

// dialFuture represents one in-flight connection attempt for a node. The
// result is published before done is closed, so all waiters observe the same
// client or typed error without holding Router.mu during the dial.
type dialFuture struct {
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	address    string
	generation uint64

	client pb.CacheServiceClient
	err    error
	once   sync.Once
}

func (d *dialFuture) complete(client pb.CacheServiceClient, err error) {
	d.once.Do(func() {
		d.client = client
		d.err = err
		close(d.done)
	})
}

func (d *dialFuture) wait() (pb.CacheServiceClient, error) {
	<-d.done
	return d.client, d.err
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
	ring        Ring
	clients     map[string]*clientState
	dialFutures map[string]*dialFuture
	generations map[string]uint64
	localID     string
	config      *RouterConfig
	mu          sync.RWMutex
}

// NewRouter creates a new router with the default configuration
func NewRouter(ring Ring, localID string) *Router {
	return NewRouterWithConfig(ring, localID, DefaultRouterConfig())
}

// NewRouterWithConfig creates a new router with a custom configuration
func NewRouterWithConfig(ring Ring, localID string, config *RouterConfig) *Router {
	if config == nil {
		config = DefaultRouterConfig()
	}
	return &Router{
		ring:        ring,
		clients:     make(map[string]*clientState),
		dialFutures: make(map[string]*dialFuture),
		generations: make(map[string]uint64),
		localID:     localID,
		config:      config,
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
		metrics.ClusterRouteRequests.WithLabelValues("error").Inc()
		return nil, err
	}

	// Defensive check: callers should use IsLocal() before calling Route()
	// If we get here with a local key, it's likely a bug in the caller
	if node.ID == r.localID {
		zlog.Warn().
			Str("key", key).
			Str("node_id", node.ID).
			Msg("Received request to route to local node")

		metrics.ClusterRouteRequests.WithLabelValues("local").Inc()
		metrics.ClusterRoutingErrors.WithLabelValues("local_routing").Inc()
		return nil, NewLocalRoutingError(r.localID, key)
	}

	var lastErr error
	backoff := r.config.InitialRetryBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry with jittered exponential backoff. Jitter spreads
			// retries so a degraded ring's survivors don't all reconnect to a
			// recovered node in lockstep (thundering herd, issue #164).
			time.Sleep(jittered(backoff))
			backoff = r.calculateBackoff(backoff)

			metrics.ClusterRetryAttempts.WithLabelValues(node.ID).Inc()

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

			metrics.ClusterRouteRequests.WithLabelValues("remote").Inc()
			return client, nil
		}

		lastErr = err

		// Don't retry if it's not a retryable error
		if !IsRetryableError(err) {
			break
		}
	}

	metrics.ClusterRouteRequests.WithLabelValues("error").Inc()
	metrics.ClusterRoutingErrors.WithLabelValues("max_retries_exceeded").Inc()
	return nil, NewMaxRetriesExceededError(node.ID, key, maxRetries+1, lastErr)
}

// getClient returns a client for the given node, creating one if necessary.
// Connection establishment is coordinated per node so a slow dial cannot hold
// up healthy cached clients for other nodes.
func (r *Router) getClient(nodeID string) (pb.CacheServiceClient, error) {
	// Fast path: keep the map read lock while checking the state. Connection
	// fields are published under the same lock by runDial, so this avoids a
	// race with reconnect, removal, or refresh without extending the critical
	// section into any network operation.
	r.mu.RLock()
	state, exists := r.clients[nodeID]
	if exists && state != nil {
		if err := r.getConnectionHealth(state, nodeID); err == nil {
			client := state.client
			r.mu.RUnlock()
			return client, nil
		} else if errors.Is(err, ErrCircuitBreakerOpen) {
			r.mu.RUnlock()
			metrics.ClusterRoutingErrors.WithLabelValues("circuit_breaker_open").Inc()
			return nil, err
		}
	}
	r.mu.RUnlock()

	zlog.Debug().
		Str("node_id", nodeID).
		Msg("Checking if client exists and is healthy")

	return r.startOrJoinDial(nodeID)
}

// startOrJoinDial rechecks the client state under Router.mu, then either joins
// the current node-specific dial or installs a new future. The actual dial is
// always performed after the map lock is released.
func (r *Router) startOrJoinDial(nodeID string) (pb.CacheServiceClient, error) {
	r.mu.Lock()

	zlog.Debug().
		Str("node_id", nodeID).
		Msg("Creating new client or reconnecting")

	state, exists := r.clients[nodeID]
	if exists && state != nil {
		if err := r.getConnectionHealth(state, nodeID); err == nil {
			client := state.client
			r.mu.Unlock()
			return client, nil
		} else if errors.Is(err, ErrCircuitBreakerOpen) {
			r.mu.Unlock()
			return nil, err
		}
	}

	if future := r.dialFutures[nodeID]; future != nil {
		r.mu.Unlock()
		return future.wait()
	}

	nodeAddr := r.nodeAddressLocked(nodeID)
	if nodeAddr == "" {
		logsample.DegradedRing().
			Str("node_id", nodeID).
			Msg("Node not found in ring")

		metrics.ClusterRoutingErrors.WithLabelValues("node_not_found").Inc()
		r.mu.Unlock()
		return nil, NewNodeNotFoundError(nodeID, "")
	}

	if state == nil {
		state = &clientState{}
		r.clients[nodeID] = state
	}

	// Detach the old connection before handing the state to the new dial. It
	// is closed after releasing Router.mu, and a concurrent healthy lookup can
	// no longer return it while the reconnect is in progress.
	oldConn := state.conn
	state.conn = nil
	state.client = nil

	generation := r.generations[nodeID] + 1
	r.generations[nodeID] = generation
	dialCtx, cancel := context.WithCancel(context.Background())
	future := &dialFuture{
		done:       make(chan struct{}),
		ctx:        dialCtx,
		cancel:     cancel,
		address:    nodeAddr,
		generation: generation,
	}
	r.dialFutures[nodeID] = future
	r.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}

	go r.runDial(nodeID, state, future)
	return future.wait()
}

func (r *Router) nodeAddressLocked(nodeID string) string {
	for _, node := range r.ring.GetAllNodes() {
		if node.ID == nodeID {
			// Use listen address for client connections.
			return node.ListenAddress
		}
	}
	return ""
}

func (r *Router) isCurrentDialLocked(nodeID string, state *clientState, future *dialFuture) bool {
	return r.clients[nodeID] == state &&
		r.dialFutures[nodeID] == future &&
		r.generations[nodeID] == future.generation
}

func (r *Router) runDial(nodeID string, state *clientState, future *dialFuture) {
	defer future.cancel()

	conn, err := r.createConnection(future.ctx, future.address)
	if err != nil {
		r.mu.Lock()
		if !r.isCurrentDialLocked(nodeID, state, future) {
			r.mu.Unlock()
			future.complete(nil, NewConnectionFailedError(nodeID, future.address, err))
			return
		}

		delete(r.dialFutures, nodeID)
		r.recordFailureAndOpenCircuit(state, nodeID)
		routeErr := NewConnectionFailedError(nodeID, future.address, err)
		future.complete(nil, routeErr)
		r.mu.Unlock()

		logsample.DegradedRing().
			Str("node_id", nodeID).
			Str("address", future.address).
			Msg("Failed to create connection to node")

		metrics.ClusterConnectionFailures.WithLabelValues(nodeID, "connection_failed").Inc()
		metrics.ClusterRoutingErrors.WithLabelValues("connection_failed").Inc()
		return
	}

	client := pb.NewCacheServiceClient(conn)
	r.mu.Lock()
	if !r.isCurrentDialLocked(nodeID, state, future) {
		r.mu.Unlock()
		_ = conn.Close()
		future.complete(nil, NewConnectionFailedError(nodeID, future.address, context.Canceled))
		return
	}

	delete(r.dialFutures, nodeID)
	state.client = client
	state.conn = conn
	atomic.StoreInt32(&state.failureCount, 0) // Reset failure count on successful connection
	metrics.ClusterConnectionsActive.WithLabelValues(nodeID).Set(1)
	future.complete(client, nil)
	r.mu.Unlock()

	zlog.Debug().
		Str("node_id", nodeID).
		Str("address", future.address).
		Msg("Created connection to node")
}

// createConnection creates a new gRPC connection with configured parameters
func (r *Router) createConnection(parent context.Context, address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(parent, r.config.ConnectionTimeout)
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

	// Build dial options with defaults, then append any custom options
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(r.config.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(r.config.MaxSendMsgSize),
		),
		grpc.WithBlock(),
	}
	dialOpts = append(dialOpts, r.config.GRPCDialOptions...)

	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for node %s: %w", address, err)
	}

	zlog.Debug().
		Str("address", address).
		Msg("Created connection to node")

	return conn, nil
}

func (r *Router) getConnectionHealth(state *clientState, nodeID string) error {
	// Check if circuit breaker is open
	if r.isCircuitOpen(state, nodeID) {
		logsample.DegradedRing().
			Str("node_id", nodeID).
			Msg("Circuit breaker open for node")

		return NewCircuitBreakerOpenError(nodeID)
	}

	// Check connection state
	error := fmt.Errorf("client state not set for node %s", nodeID)
	if state.conn != nil {
		connState := state.conn.GetState()
		if connState != connectivity.Shutdown && connState != connectivity.TransientFailure {
			return nil
		}
		error = fmt.Errorf("connection state is %s for node %s", connState.String(), nodeID)
	}

	return error
}

// isCircuitOpen checks if the circuit breaker is open for a client
// nodeID parameter is used for metrics reporting when circuit closes
func (r *Router) isCircuitOpen(state *clientState, nodeID string) bool {
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

			// Update metrics using the provided nodeID
			metrics.ClusterCircuitBreakerState.WithLabelValues(nodeID).Set(0)
		}
		return false
	}

	return true
}

// recordFailureAndOpenCircuit records a failure and potentially opens the circuit breaker
// nodeID parameter is used for metrics reporting
func (r *Router) recordFailureAndOpenCircuit(state *clientState, nodeID string) {
	failures := atomic.AddInt32(&state.failureCount, 1)

	state.mu.Lock()
	state.lastFailure = time.Now()
	state.mu.Unlock()

	if failures >= int32(r.config.CircuitBreakerThreshold) {
		if atomic.CompareAndSwapInt32(&state.circuitOpen, 0, 1) {
			state.mu.Lock()
			state.circuitOpenTime = time.Now()
			state.mu.Unlock()

			// Update metrics using the provided nodeID
			metrics.ClusterCircuitBreakerOpened.WithLabelValues(nodeID).Inc()
			metrics.ClusterCircuitBreakerState.WithLabelValues(nodeID).Set(1)

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

// jittered applies equal jitter to a backoff duration, returning a random value
// in [d/2, d]. Randomizing each node's retry sleep keeps a degraded ring's
// survivors from retrying (and reconnecting to a recovered node) in lockstep,
// which would otherwise re-concentrate the thundering herd on every attempt.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func (r *Router) IsLocal(key string) bool {
	return r.ring.IsLocal(key)
}

// GetClientForNode returns a client for a specific node ID
// This is useful for operations that need to query all nodes (e.g., List)
func (r *Router) GetClientForNode(nodeID string) (pb.CacheServiceClient, error) {
	// Check if this is the local node (shouldn't be called for local, but check defensively)
	if nodeID == r.localID {
		return nil, NewLocalRoutingError(r.localID, "")
	}

	return r.getClient(nodeID)
}

// RemoveClient removes and closes the client connection for a node. Any
// in-flight dial is invalidated before the map entry is removed, so a stale
// result cannot repopulate the router after removal.
func (r *Router) RemoveClient(nodeID string) {
	var conn *grpc.ClientConn
	var future *dialFuture
	removed := false

	r.mu.Lock()
	r.generations[nodeID]++
	if current, exists := r.dialFutures[nodeID]; exists {
		delete(r.dialFutures, nodeID)
		future = current
		current.complete(nil, NewConnectionFailedError(nodeID, current.address, context.Canceled))
	}
	if state, exists := r.clients[nodeID]; exists {
		if state != nil {
			conn = state.conn
		}
		delete(r.clients, nodeID)
		metrics.ClusterConnectionsActive.WithLabelValues(nodeID).Set(0)
		removed = true
	}
	r.mu.Unlock()

	if future != nil {
		future.cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if removed {
		zlog.Debug().
			Str("node_id", nodeID).
			Msg("Removed client connection")
	}
}

// Close closes all client connections and cancels in-flight dials. It leaves
// the Router reusable, matching the previous behavior after the client map was
// cleared, while generations prevent old dial results from being published.
func (r *Router) Close() error {
	var conns []*grpc.ClientConn
	var futures []*dialFuture

	r.mu.Lock()
	for nodeID, state := range r.clients {
		r.generations[nodeID]++
		if state != nil && state.conn != nil {
			conns = append(conns, state.conn)
		}
	}
	for nodeID, future := range r.dialFutures {
		r.generations[nodeID]++
		future.complete(nil, NewConnectionFailedError(nodeID, future.address, context.Canceled))
		futures = append(futures, future)
	}
	r.clients = make(map[string]*clientState)
	r.dialFutures = make(map[string]*dialFuture)
	r.mu.Unlock()

	for _, future := range futures {
		future.cancel()
	}
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			zlog.Error().
				Err(err).
				Msg("Error closing connection")
		}
	}

	zlog.Debug().
		Msg("Closed all client connections")

	return nil
}

// RefreshConnections removes connections to inactive nodes. It also
// invalidates pending dials for those nodes, then performs cancellation and
// connection closes without Router.mu held.
func (r *Router) RefreshConnections() {
	var conns []*grpc.ClientConn
	var futures []*dialFuture

	r.mu.Lock()

	zlog.Debug().
		Msg("Refreshing connections")

	// Get active nodes
	activeNodes := r.ring.GetActiveNodes()
	activeNodeMap := make(map[string]bool)
	for _, node := range activeNodes {
		activeNodeMap[node.ID] = true
	}

	for nodeID, future := range r.dialFutures {
		if !activeNodeMap[nodeID] {
			r.generations[nodeID]++
			delete(r.dialFutures, nodeID)
			future.complete(nil, NewConnectionFailedError(nodeID, future.address, context.Canceled))
			futures = append(futures, future)
		}
	}

	// Remove connections to inactive nodes
	for nodeID, state := range r.clients {
		if !activeNodeMap[nodeID] {
			r.generations[nodeID]++
			if state != nil && state.conn != nil {
				conns = append(conns, state.conn)
			}
			delete(r.clients, nodeID)
		}
	}
	r.mu.Unlock()

	for _, future := range futures {
		future.cancel()
	}
	for _, conn := range conns {
		_ = conn.Close()
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
		if _, dialing := r.dialFutures[nodeID]; dialing {
			// A pending router dial has no ClientConn yet, so the zero-value
			// connectivity.State would otherwise be reported as IDLE.
			connState = connectivity.Connecting
		} else if state.conn != nil {
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
