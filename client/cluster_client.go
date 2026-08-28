// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

const (
	// minRefreshInterval is the minimum time between epoch mismatch refresh triggers.
	// This prevents a burst of requests from triggering many concurrent refreshes.
	minRefreshInterval = 100 * time.Millisecond
)

// connectionSet is immutable after publication. Readers can safely use its map
// without a lock because writers clone the map before publishing a new set.
type connectionSet struct {
	conns map[string]*connection // address -> connection
	ring  *ringState
}

// connectionTransition marks a topology refresh whose replacement pools are
// still being staged. Routes for addresses removed by the target topology wait
// on done instead of using a stale pool; routes for unchanged addresses remain
// lock-free while staging proceeds.
type connectionTransition struct {
	activeAddrs map[string]bool
	done        chan struct{}
}

// ClusterClient implements a cluster-aware cache client with topology support
type ClusterClient struct {
	*Operations // Embedded for shared operations
	conns       atomic.Pointer[connectionSet]
	transition  atomic.Pointer[connectionTransition]
	topology    *TopologyManager // Manages topology
	config      *ClientConfig
	seedAddrs   []string // Seed addresses for bootstrap
	currentIdx  atomic.Uint32

	// mu serializes connection-set publication and client close. Request routing
	// only loads conns atomically and never waits for this mutex.
	mu            sync.Mutex
	topologyMu    sync.Mutex // Serializes topology staging without blocking routes.
	updates       sync.WaitGroup
	closed        atomic.Bool
	stopCh        chan struct{}
	refreshCancel context.CancelFunc
	stagingCtx    context.Context
	stagingCancel context.CancelFunc

	// lastRefresh tracks when the last topology refresh was triggered.
	// Used to rate-limit epoch mismatch refresh triggers.
	lastRefresh atomic.Int64
}

// NewClusterClient creates a new ClusterClient with the given configuration
func NewClusterClient(config *ClientConfig) (*ClusterClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.Addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}

	config.SetDefaults()

	// Fetch initial topology
	topology, err := NewTopologyManager(config.Addrs, config.RefreshInterval, config.DialOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create topology manager: %w", err)
	}

	stagingCtx, stagingCancel := context.WithCancel(context.Background())
	client := &ClusterClient{
		topology:      topology,
		config:        config,
		seedAddrs:     config.Addrs,
		stopCh:        make(chan struct{}),
		stagingCtx:    stagingCtx,
		stagingCancel: stagingCancel,
	}
	client.conns.Store(&connectionSet{
		conns: make(map[string]*connection),
		ring:  topology.ring.snapshot(),
	})

	// Initialize connections based on topology
	if err := client.updateConnections(); err != nil {
		// Clean up any created connections
		client.Close()
		return nil, fmt.Errorf("failed to initialize connections: %w", err)
	}

	// Initialize operations with this client as the router
	client.Operations = NewOperations(client)

	// Start topology refresh goroutine
	refreshCtx, cancel := context.WithCancel(context.Background())
	client.refreshCancel = cancel
	go client.topology.TopologyRefreshLoopWithTopology(refreshCtx, func(topology *clusterpb.ClusterTopology) {
		// Stage connections before publishing the refreshed ring.
		client.updateTopologyAndConnections(topology)
	})

	return client, nil
}

// closeConnectionList closes pools after they have been removed from the
// published set. Closing is deliberately outside the publisher mutex so a
// topology change never makes readers wait for pool lifecycle work.
func closeConnectionList(conns []*connection) error {
	var firstErr error
	for _, conn := range conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// beginUpdate registers pool staging so Close can wait for it without
// allowing a new staging operation to begin after the client is closed.
func (c *ClusterClient) beginUpdate() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return false
	}
	c.updates.Add(1)
	return true
}

// updateConnections reconciles the initial connection set with the already
// published topology.
func (c *ClusterClient) updateConnections() error {
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()

	if !c.beginUpdate() {
		return nil
	}
	defer c.updates.Done()

	nodeAddrs, generation := c.topology.getNodeAddressesAndEpoch()
	activeAddrs := make(map[string]bool, len(nodeAddrs))
	for _, addr := range nodeAddrs {
		activeAddrs[addr] = true
	}
	return c.reconcileConnections(activeAddrs, generation, nil, nil)
}

// updateTopologyAndConnections stages pools for topology before publishing the
// new ring. Keeping the topology update serialized prevents two staged sets
// from crossing while leaving request routing independent of that serialization.
func (c *ClusterClient) updateTopologyAndConnections(topology *clusterpb.ClusterTopology) error {
	return c.updateTopologyAndConnectionsContext(nil, topology)
}

// updateTopologyAndConnectionsContext is the synchronous form used by
// operation retries. Its context bounds both topology reconciliation and any
// blocking pool dials; a nil context uses the client lifecycle context.
func (c *ClusterClient) updateTopologyAndConnectionsContext(ctx context.Context, topology *clusterpb.ClusterTopology) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.topologyMu.Unlock()

	update, changed := c.topology.prepareTopology(topology)
	if !changed && topology != nil && c.topology.shouldRetryTopology(topology.Epoch) {
		update, changed = c.topology.prepareTopologyForRetry(topology)
	}
	if !changed {
		return nil
	}
	if !c.beginUpdate() {
		return nil
	}
	defer c.updates.Done()

	return c.reconcileConnections(update.activeAddrs, update.baseEpoch, update, ctx)
}

// reconcileConnections stages missing pools and publishes a connection set.
// A topology update first prepares its immutable ring and pools, then publishes
// both in one connectionSet pointer. Readers therefore use a matching ring and
// address map even when they overlap the publication.
func (c *ClusterClient) reconcileConnections(activeAddrs map[string]bool, generation uint64, update *preparedTopology, operationCtx context.Context) error {
	stagingCtx, cancelStaging := c.stagingContextFor(operationCtx)
	defer cancelStaging()

	var transition *connectionTransition
	if update != nil {
		transition = &connectionTransition{
			activeAddrs: activeAddrs,
			done:        make(chan struct{}),
		}
		c.transition.Store(transition)
		defer c.finishConnectionTransition(transition)
	}

	current := c.conns.Load()
	if current == nil {
		current = &connectionSet{
			conns: make(map[string]*connection),
			ring:  c.topology.ring.snapshot(),
		}
	}

	staged := make(map[string]*connection)
	stageFailed := false
	for addr := range activeAddrs {
		if _, exists := current.conns[addr]; exists {
			continue
		}
		if c.closed.Load() {
			for _, stagedConn := range staged {
				_ = stagedConn.close()
			}
			return nil
		}

		conn, err := newConnectionWithEpochContext(
			stagingCtx,
			addr,
			c.config.DialOpts,
			c.config.ConnectionPoolSize,
			c.topology.GetTopologyEpoch, // Epoch getter
			c.onEpochMismatch,           // Mismatch handler
		)
		if err != nil {
			// Keep any pools that did initialize. A later refresh can retry
			// the missing pool without taking reachable members offline.
			stageFailed = true
			continue
		}
		staged[addr] = conn
	}

	if c.closed.Load() {
		_ = closeConnectionList(connectionValues(staged))
		return nil
	}
	if err := stagingCtx.Err(); err != nil {
		_ = closeConnectionList(connectionValues(staged))
		return err
	}
	if stageFailed && update == nil {
		c.publishReconciledConnections(activeAddrs, current.ring, staged)
		c.topology.requestTopologyRetry(generation)
		return nil
	}

	if update == nil {
		// This path is used only for initial setup. Do not reconcile a set
		// built from a topology that changed while pools were being staged.
		if c.topology.GetTopologyEpoch() != generation {
			_ = closeConnectionList(connectionValues(staged))
			return nil
		}

		c.publishReconciledConnections(activeAddrs, current.ring, staged)
		return nil
	}

	// Publish the ring only after all missing-pool dial attempts finish. The
	// connectionSet still contains the old ring until the matching set is
	// stored below, so Route remains on a coherent snapshot throughout. If a
	// dial failed, its active address remains absent from the set and routes
	// to that unavailable member return the existing routing error.
	if !c.topology.applyPreparedTopology(update) {
		_ = closeConnectionList(connectionValues(staged))
		return nil
	}

	c.publishReconciledConnections(activeAddrs, update.ring, staged)
	if stageFailed {
		c.topology.requestTopologyRetry(update.epoch)
	} else {
		c.topology.clearTopologyRetry()
	}
	return nil
}

// publishReconciledConnections atomically replaces the address set and ring,
// then retires pools that are absent from the attempted topology outside the
// publisher mutex. A failed dial is represented by a missing pool in the new
// set, so reachable active members remain available without retaining removed
// members across refreshes.
func (c *ClusterClient) publishReconciledConnections(activeAddrs map[string]bool, ring *ringState, staged map[string]*connection) {
	var retired, discarded []*connection
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = closeConnectionList(connectionValues(staged))
		return
	}
	current := c.conns.Load()
	if current == nil {
		current = &connectionSet{
			conns: make(map[string]*connection),
			ring:  c.topology.ring.snapshot(),
		}
	}
	if ring == nil {
		ring = current.ring
	}
	next := make(map[string]*connection, len(activeAddrs))
	for addr, conn := range current.conns {
		if activeAddrs[addr] {
			next[addr] = conn
		} else {
			retired = append(retired, conn)
		}
	}
	for addr, conn := range staged {
		if existing, exists := next[addr]; exists {
			if existing != conn {
				discarded = append(discarded, conn)
			}
			continue
		}
		next[addr] = conn
	}
	c.conns.Store(&connectionSet{conns: next, ring: ring})
	c.mu.Unlock()

	c.finishConnectionTransition(c.transition.Load())
	_ = closeConnectionList(retired)
	_ = closeConnectionList(discarded)
}

func (c *ClusterClient) stagingContext() context.Context {
	if c.stagingCtx == nil {
		return context.Background()
	}
	return c.stagingCtx
}

// lockTopology serializes topology staging while allowing synchronous
// operation retries to stop waiting when their context expires.
func (c *ClusterClient) lockTopology(ctx context.Context) error {
	if ctx == nil || ctx.Done() == nil {
		c.topologyMu.Lock()
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if c.topologyMu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// stagingContextFor combines an operation context with the client lifecycle.
// Synchronous retries stop at the operation deadline, while Close still
// interrupts them when the caller supplied a longer-lived context.
func (c *ClusterClient) stagingContextFor(operationCtx context.Context) (context.Context, context.CancelFunc) {
	if operationCtx == nil {
		return c.stagingContext(), func() {}
	}

	lifecycleCtx := c.stagingContext()
	if lifecycleCtx.Done() == nil {
		return operationCtx, func() {}
	}

	stagingCtx, cancel := context.WithCancel(operationCtx)
	stopLifecycle := context.AfterFunc(lifecycleCtx, cancel)
	return stagingCtx, func() {
		stopLifecycle()
		cancel()
	}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (c *ClusterClient) finishConnectionTransition(transition *connectionTransition) {
	if transition == nil {
		return
	}
	if c.transition.CompareAndSwap(transition, nil) {
		close(transition.done)
	}
}

func connectionValues(conns map[string]*connection) []*connection {
	values := make([]*connection, 0, len(conns))
	for _, conn := range conns {
		values = append(values, conn)
	}
	return values
}

// onEpochMismatch is called when server returns a different epoch than client.
// With content-addressable epochs, ANY mismatch means the ring state differs,
// so we trigger a background topology refresh.
//
// Rate limiting: To prevent a burst of mismatches from triggering many concurrent
// refreshes, we enforce a minimum interval between refresh attempts.
func (c *ClusterClient) onEpochMismatch(clientEpoch, serverEpoch uint64) {
	if serverEpoch == clientEpoch {
		return
	}

	// Rate limit: check if enough time has passed since last refresh
	now := time.Now().UnixNano()
	lastRefresh := c.lastRefresh.Load()
	if now-lastRefresh < int64(minRefreshInterval) {
		// Too soon since last refresh, skip
		return
	}

	// Try to claim the refresh slot using CAS to prevent concurrent refreshes
	if !c.lastRefresh.CompareAndSwap(lastRefresh, now) {
		// Another goroutine already claimed it, skip
		return
	}

	go c.forceRefreshTopology(context.Background())
}

// Route determines which connection to use for a given key
// Implements Router interface
// Optimized to minimize lock contention using cached routing decisions
func (c *ClusterClient) Route(key string) (*connection, error) {
	for {
		transition := c.transition.Load()
		set := c.conns.Load()
		if set == nil {
			return nil, fmt.Errorf("no connection for key %s", key)
		}

		addr, err := getNodeForKeyFromState(set.ring, key)
		if err != nil {
			return nil, err
		}

		// Recheck the pointers after calculating the owner. If publication
		// crossed the read, retry against the new coherent snapshot.
		if c.transition.Load() != transition || c.conns.Load() != set {
			continue
		}
		if transition != nil && !transition.activeAddrs[addr] {
			<-transition.done
			continue
		}

		conn, exists := set.conns[addr]
		if !exists {
			return nil, fmt.Errorf("no connection for address %s", addr)
		}
		return conn, nil
	}
}

// RouteContext is Route with cancellation while waiting for a topology
// transition to publish its replacement pools.
func (c *ClusterClient) RouteContext(ctx context.Context, key string) (*connection, error) {
	if ctx == nil {
		return c.Route(key)
	}
	waitDone := ctx.Done()
	if waitDone == nil {
		return c.Route(key)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		transition := c.transition.Load()
		set := c.conns.Load()
		if set == nil {
			return nil, fmt.Errorf("no connection for key %s", key)
		}

		addr, err := getNodeForKeyFromState(set.ring, key)
		if err != nil {
			return nil, err
		}

		// Recheck the pointers after calculating the owner. If publication
		// crossed the read, retry against the new coherent snapshot.
		if c.transition.Load() != transition || c.conns.Load() != set {
			continue
		}
		if transition != nil && !transition.activeAddrs[addr] {
			select {
			case <-transition.done:
				continue
			case <-waitDone:
				return nil, ctx.Err()
			}
		}

		conn, exists := set.conns[addr]
		if !exists {
			return nil, fmt.Errorf("no connection for address %s", addr)
		}
		return conn, nil
	}
}

// RoundRobinRoute selects a connection using round-robin (for List operation)
// Implements Router interface
func (c *ClusterClient) RoundRobinRoute() (*connection, error) {
	for {
		transition := c.transition.Load()
		set := c.conns.Load()
		if set == nil {
			return nil, fmt.Errorf("no available connections")
		}

		// Exclude pools removed by an in-flight refresh. If no old pool is
		// still active, wait for publication rather than selecting a stale one.
		addresses := make([]string, 0, len(set.conns))
		for addr := range set.conns {
			if transition == nil || transition.activeAddrs[addr] {
				addresses = append(addresses, addr)
			}
		}
		if c.transition.Load() != transition || c.conns.Load() != set {
			continue
		}
		if len(addresses) == 0 {
			if transition != nil {
				<-transition.done
				continue
			}
			return nil, fmt.Errorf("no available connections")
		}

		// Get all connections and sort for consistent ordering.
		sort.Strings(addresses)
		idx := c.currentIdx.Add(1) - 1
		addr := addresses[idx%uint32(len(addresses))]
		return set.conns[addr], nil
	}
}

// RoundRobinRouteContext is RoundRobinRoute with cancellation while waiting
// for a topology transition to publish its replacement pools.
func (c *ClusterClient) RoundRobinRouteContext(ctx context.Context) (*connection, error) {
	if ctx == nil {
		return c.RoundRobinRoute()
	}
	waitDone := ctx.Done()
	if waitDone == nil {
		return c.RoundRobinRoute()
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		transition := c.transition.Load()
		set := c.conns.Load()
		if set == nil {
			return nil, fmt.Errorf("no available connections")
		}

		// Exclude pools removed by an in-flight refresh. If no old pool is
		// still active, wait for publication rather than selecting a stale one.
		addresses := make([]string, 0, len(set.conns))
		for addr := range set.conns {
			if transition == nil || transition.activeAddrs[addr] {
				addresses = append(addresses, addr)
			}
		}
		if c.transition.Load() != transition || c.conns.Load() != set {
			continue
		}
		if len(addresses) == 0 {
			if transition != nil {
				select {
				case <-transition.done:
					continue
				case <-waitDone:
					return nil, ctx.Err()
				}
			}
			return nil, fmt.Errorf("no available connections")
		}

		// Get all connections and sort for consistent ordering.
		sort.Strings(addresses)
		idx := c.currentIdx.Add(1) - 1
		addr := addresses[idx%uint32(len(addresses))]
		return set.conns[addr], nil
	}
}

// forceRefreshTopology attempts to refresh topology and update connections.
func (c *ClusterClient) forceRefreshTopology(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false
	}

	fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
	topology, err := c.topology.FetchTopology(fetchCtx)
	cancel()
	if err != nil {
		return false
	}

	if err := c.updateTopologyAndConnectionsContext(ctx, topology); err != nil {
		return false
	}
	return ctx.Err() == nil
}

// Put stores a value with retry logic for routing errors
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	err := c.Operations.Put(ctx, key, data, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		if c.forceRefreshTopology(ctx) {
			return c.Operations.Put(ctx, key, data, ttlSeconds)
		}
		if ctxErr := contextError(ctx); ctxErr != nil {
			return ctxErr
		}
	}

	return err
}

// Get retrieves a value with retry logic
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	// We need custom logic to track if partial data was received
	// Pass end=0 to indicate reading to EOF (end <= 0 means read to EOF)
	return c.getDataWithRetry(ctx, key, 0, 0, 1)
}

// GetRange retrieves a byte range with retry logic
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	// Use the unified retry logic with range parameters
	return c.getDataWithRetry(ctx, key, start, end, 1)
}

// getDataWithRetry implements Get/GetRange with retry logic that tracks partial data
// For regular Get operations, pass start=0 and end=0
// For GetRange operations, pass the actual start and end values
func (c *ClusterClient) getDataWithRetry(ctx context.Context, key string, start, end int64, retryCount int) ([]byte, error) {
	conn, err := c.RouteContext(ctx, key)
	if err != nil {
		return nil, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	// Build request with optional range parameters
	req := &pb.GetRequest{Key: key}
	if start != 0 || end != 0 {
		req.Start = start
		req.End = end
	}

	stream, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, DefaultBufferSize)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Only retry if:
			// 1. It's a routing error
			// 2. We haven't received any data yet (to avoid data corruption)
			// 3. We have retries remaining
			if isRoutingError(err) && len(result) == 0 && retryCount > 0 {
				if c.forceRefreshTopology(ctx) {
					return c.getDataWithRetry(ctx, key, start, end, retryCount-1)
				}
				if ctxErr := contextError(ctx); ctxErr != nil {
					return nil, ctxErr
				}
			}
			return nil, err
		}
		result = append(result, resp.Data...)
	}
	return result, nil
}

// GetStream streams a value with retry logic
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	// We need custom logic to track if partial data was written
	// Pass end=0 to indicate reading to EOF (end <= 0 means read to EOF)
	return c.getStreamDataWithRetry(ctx, key, 0, 0, w, 1)
}

// GetRangeStream streams a byte range with retry logic
func (c *ClusterClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	// Use the unified retry logic with range parameters
	return c.getStreamDataWithRetry(ctx, key, start, end, w, 1)
}

// getStreamDataWithRetry implements GetStream/GetRangeStream with retry logic that tracks partial data
// For regular GetStream operations, pass start=0 and end=0
// For GetRangeStream operations, pass the actual start and end values
func (c *ClusterClient) getStreamDataWithRetry(ctx context.Context, key string, start, end int64, w io.Writer, retryCount int) error {
	conn, err := c.RouteContext(ctx, key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	// Build request with optional range parameters
	req := &pb.GetRequest{Key: key}
	if start != 0 || end != 0 {
		req.Start = start
		req.End = end
	}

	stream, err := client.Get(ctx, req)
	if err != nil {
		return err
	}

	var bytesWritten int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Only retry if:
			// 1. It's a routing error
			// 2. We haven't written any data yet (to avoid data corruption)
			// 3. We have retries remaining
			if isRoutingError(err) && bytesWritten == 0 && retryCount > 0 {
				if c.forceRefreshTopology(ctx) {
					return c.getStreamDataWithRetry(ctx, key, start, end, w, retryCount-1)
				}
				if ctxErr := contextError(ctx); ctxErr != nil {
					return ctxErr
				}
			}
			return err
		}
		n, err := w.Write(resp.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
	}
	return nil
}

// Delete removes a key with retry logic
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	err := c.Operations.Delete(ctx, key)

	// Retry once with topology refresh for routing errors
	if isRoutingError(err) {
		if c.forceRefreshTopology(ctx) {
			return c.Operations.Delete(ctx, key)
		}
		if ctxErr := contextError(ctx); ctxErr != nil {
			return ctxErr
		}
	}

	return err
}

// PutStream, List, ListPage and ListPageWithValues are inherited from Operations

// Close closes all connections and stops background goroutines
func (c *ClusterClient) Close() error {
	// Stop refresh goroutine
	if c.refreshCancel != nil {
		c.refreshCancel()
	}

	c.mu.Lock()
	if c.closed.Swap(true) {
		c.mu.Unlock()
		c.updates.Wait()
		return nil
	}

	if c.stagingCancel != nil {
		c.stagingCancel()
	}

	current := c.conns.Load()
	var currentRing *ringState
	if current != nil {
		currentRing = current.ring
	}
	current = c.conns.Swap(&connectionSet{
		conns: make(map[string]*connection),
		ring:  currentRing,
	})
	close(c.stopCh)
	c.mu.Unlock()

	// An update can be staging pools outside mu. Wait for it to observe the
	// closed state and retire those pools before returning from Close.
	c.updates.Wait()

	if current == nil {
		return nil
	}

	connections := make([]*connection, 0, len(current.conns))
	for _, conn := range current.conns {
		connections = append(connections, conn)
	}
	return closeConnectionList(connections)
}

// GetMode returns the connection mode
func (c *ClusterClient) GetMode() ConnectionMode {
	return ModeCluster
}

// GetConnectedNodes returns the addresses of all connected nodes
func (c *ClusterClient) GetConnectedNodes() []string {
	set := c.conns.Load()
	if set == nil {
		return nil
	}

	nodes := make([]string, 0, len(set.conns))
	for addr := range set.conns {
		nodes = append(nodes, addr)
	}
	sort.Strings(nodes)
	return nodes
}

// GetTopologyEpoch returns the current topology epoch
func (c *ClusterClient) GetTopologyEpoch() uint64 {
	return c.topology.GetTopologyEpoch()
}

// GetNodeIDForKey returns the node ID that owns the given key in the
// published connection-set snapshot.
func (c *ClusterClient) GetNodeIDForKey(key string) (string, error) {
	set := c.conns.Load()
	if set == nil {
		return "", fmt.Errorf("ring is empty")
	}
	return getNodeIDForKeyFromState(set.ring, key)
}

// GetNodeInfoForKey returns both the node ID and address that owns the given key
// in the published connection-set snapshot.
func (c *ClusterClient) GetNodeInfoForKey(key string) (nodeID, address string, err error) {
	set := c.conns.Load()
	if set == nil {
		return "", "", fmt.Errorf("ring is empty")
	}
	return getNodeInfoForKeyFromState(set.ring, key)
}

// FetchClusterState fetches the current cluster state from any available node.
func (c *ClusterClient) FetchClusterState() (*clusterpb.ClusterState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()
	return c.fetchClusterStateFromAddresses(ctx)
}

// fetchClusterStateFromAddresses tries to fetch cluster state from available nodes.
func (c *ClusterClient) fetchClusterStateFromAddresses(ctx context.Context) (*clusterpb.ClusterState, error) {
	// Try topology nodes first
	nodeAddresses := c.topology.GetNodeAddresses()
	for _, addr := range nodeAddresses {
		state, err := c.fetchClusterStateFromAddress(ctx, addr)
		if err == nil {
			return state, nil
		}
	}

	// Fall back to seed addresses
	for _, addr := range c.seedAddrs {
		state, err := c.fetchClusterStateFromAddress(ctx, addr)
		if err == nil {
			return state, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch cluster state from any node")
}

// fetchClusterStateFromAddress fetches cluster state from a specific address.
func (c *ClusterClient) fetchClusterStateFromAddress(ctx context.Context, addr string) (*clusterpb.ClusterState, error) {
	conn, err := grpc.NewClient(addr, c.config.DialOpts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)
	return client.GetClusterState(ctx, &clusterpb.Empty{})
}

// Test helper methods - exposed for testing only

// HasRing returns true if the published connection set has ring tokens.
func (c *ClusterClient) HasRing() bool {
	set := c.conns.Load()
	return set != nil && set.ring != nil && len(set.ring.tokens) > 0
}

// FetchTopology fetches the current topology (exposed for testing)
func (c *ClusterClient) FetchTopology() (*clusterpb.ClusterTopology, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()
	return c.topology.FetchTopology(ctx)
}

// UpdateTopology manually updates the topology (exposed for testing)
func (c *ClusterClient) UpdateTopology(topology *clusterpb.ClusterTopology) error {
	return c.updateTopologyAndConnections(topology)
}

// GetConnectionCount returns the number of active connections (exposed for testing)
func (c *ClusterClient) GetConnectionCount() int {
	set := c.conns.Load()
	if set == nil {
		return 0
	}
	return len(set.conns)
}
