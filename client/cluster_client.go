package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

// ClusterClient implements a cluster-aware cache client with topology support
type ClusterClient struct {
	*Operations                          // Embedded for shared operations
	conns         map[string]*connection // address -> connection
	topology      *TopologyManager       // Manages topology
	config        *ClientConfig
	seedAddrs     []string // Seed addresses for bootstrap
	currentIdx    atomic.Uint32
	mu            sync.RWMutex
	stopCh        chan struct{}
	refreshCancel context.CancelFunc
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

	client := &ClusterClient{
		conns:     make(map[string]*connection),
		topology:  NewTopologyManager(config.Addrs, config.RefreshInterval, config.DialOpts),
		config:    config,
		seedAddrs: config.Addrs,
		stopCh:    make(chan struct{}),
	}

	// Fetch initial topology
	err := client.topology.Initialize()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize topology: %w", err)
	}

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
	go client.topology.TopologyRefreshLoop(refreshCtx, func() {
		// Update connections when topology changes
		client.updateConnections()
	})

	return client, nil
}

// updateConnections updates connections based on current topology
func (c *ClusterClient) updateConnections() error {
	var activeNodes map[string]bool

	// Get current active nodes from topology manager
	nodeAddrs := c.topology.GetNodeAddresses()
	activeNodes = make(map[string]bool)
	for _, addr := range nodeAddrs {
		activeNodes[addr] = true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create new connections for new nodes
	for addr := range activeNodes {
		if _, exists := c.conns[addr]; !exists {
			conn, err := newConnection(addr, c.config.DialOpts, c.config.ConnectionPoolSize)
			if err != nil {
				// Log error but continue
				continue
			}
			c.conns[addr] = conn
		}
	}

	// Close connections for removed nodes
	for addr, conn := range c.conns {
		if !activeNodes[addr] {
			conn.close()
			delete(c.conns, addr)
		}
	}

	return nil
}

// Route determines which connection to use for a given key
// Implements Router interface
// Optimized to minimize lock contention using cached routing decisions
func (c *ClusterClient) Route(key string) (*connection, error) {
	// Get node address for key (uses lock-free cache)
	addr, err := c.topology.GetNodeForKey(key)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	conn, exists := c.conns[addr]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no connection for address %s", addr)
	}

	return conn, nil
}

// RoundRobinRoute selects a connection using round-robin (for List operation)
// Implements Router interface
func (c *ClusterClient) RoundRobinRoute() (*connection, error) {
	c.mu.RLock()
	if len(c.conns) == 0 {
		c.mu.RUnlock()
		return nil, fmt.Errorf("no available connections")
	}

	// Get all connections and sort for consistent ordering
	var addresses []string
	for addr := range c.conns {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)

	idx := c.currentIdx.Add(1) - 1
	addr := addresses[idx%uint32(len(addresses))]
	conn := c.conns[addr]
	c.mu.RUnlock()

	return conn, nil
}

// forceRefreshTopology attempts to refresh topology and update connections
func (c *ClusterClient) forceRefreshTopology(ctx context.Context) bool {
	fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
	_, err := c.topology.RefreshTopology(fetchCtx)
	cancel()

	if err == nil {
		c.updateConnections()
		return true
	}
	return false
}

// Put stores a value with retry logic for routing errors
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	err := c.Operations.Put(ctx, key, data, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) && c.forceRefreshTopology(ctx) {
		return c.Operations.Put(ctx, key, data, ttlSeconds)
	}

	return err
}

// Get retrieves a value with retry logic
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	// We need custom logic to track if partial data was received
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
	conn, err := c.Route(key)
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
			if isRoutingError(err) && len(result) == 0 && retryCount > 0 && c.forceRefreshTopology(ctx) {
				return c.getDataWithRetry(ctx, key, start, end, retryCount-1)
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
	conn, err := c.Route(key)
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
			if isRoutingError(err) && bytesWritten == 0 && retryCount > 0 && c.forceRefreshTopology(ctx) {
				return c.getStreamDataWithRetry(ctx, key, start, end, w, retryCount-1)
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
	if isRoutingError(err) && c.forceRefreshTopology(ctx) {
		return c.Operations.Delete(ctx, key)
	}

	return err
}

// PutStream and List are inherited from Operations

// Close closes all connections and stops background goroutines
func (c *ClusterClient) Close() error {
	// Stop refresh goroutine
	if c.refreshCancel != nil {
		c.refreshCancel()
	}

	// Close stop channel
	select {
	case <-c.stopCh:
		// Already closed
	default:
		close(c.stopCh)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.conns = make(map[string]*connection)
	return firstErr
}

// GetMode returns the connection mode
func (c *ClusterClient) GetMode() ConnectionMode {
	return ModeCluster
}

// GetConnectedNodes returns the addresses of all connected nodes
func (c *ClusterClient) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.conns))
	for addr := range c.conns {
		nodes = append(nodes, addr)
	}
	sort.Strings(nodes)
	return nodes
}

// GetTopologyEpoch returns the current topology epoch
func (c *ClusterClient) GetTopologyEpoch() uint64 {
	return c.topology.GetTopologyEpoch()
}

// HasRing returns true if the consistent hash ring is initialized
func (c *ClusterClient) HasRing() bool {
	return c.topology.HasRing()
}

// GetPartitionOwner returns the node ID that owns the given partition
func (c *ClusterClient) GetPartitionOwner(partitionID int32) string {
	return c.topology.GetPartitionOwner(partitionID)
}

// GetPartitionOwnerCount returns the number of partition owners
func (c *ClusterClient) GetPartitionOwnerCount() int {
	return c.topology.GetPartitionOwnerCount()
}

// Test helper methods - exposed for testing only

// FetchTopology fetches the current topology (exposed for testing)
func (c *ClusterClient) FetchTopology() (*clusterpb.ClusterTopology, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()
	return c.topology.FetchTopology(ctx)
}

// UpdateTopology manually updates the topology (exposed for testing)
func (c *ClusterClient) UpdateTopology(topology *clusterpb.ClusterTopology) error {
	c.topology.UpdateTopology(topology)
	return c.updateConnections()
}

// GetConnectionCount returns the number of active connections (exposed for testing)
func (c *ClusterClient) GetConnectionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns)
}
