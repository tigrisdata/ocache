package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tigrisdata/ocache/common/bufferpool"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

// ClusterClient implements a cluster-aware cache client with topology support
type ClusterClient struct {
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
		topology:  NewTopologyManager(),
		config:    config,
		seedAddrs: config.Addrs,
		stopCh:    make(chan struct{}),
	}

	// Fetch initial topology
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()

	topology, err := client.topology.FetchTopology(ctx, config.Addrs, config.DialOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch initial topology: %w", err)
	}

	// Initialize connections based on topology
	if err := client.updateConnections(topology); err != nil {
		// Clean up any created connections
		client.Close()
		return nil, fmt.Errorf("failed to initialize connections: %w", err)
	}

	// Start topology refresh goroutine
	refreshCtx, cancel := context.WithCancel(context.Background())
	client.refreshCancel = cancel
	go TopologyRefreshLoop(refreshCtx, client.topology, client.seedAddrs, config.DialOpts, config.RefreshInterval, func() {
		// Update connections when topology changes
		client.updateConnections(nil)
	})

	return client, nil
}

// updateConnections updates connections based on current topology
func (c *ClusterClient) updateConnections(topology *clusterpb.ClusterTopology) error {
	var activeNodes map[string]bool
	var updated bool

	if topology != nil {
		activeNodes, updated = c.topology.UpdateTopology(topology)
		if !updated {
			return nil // No change
		}
	} else {
		// Get current active nodes from topology manager
		nodeAddrs := c.topology.GetNodeAddresses()
		activeNodes = make(map[string]bool)
		for _, addr := range nodeAddrs {
			activeNodes[addr] = true
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create new connections for new nodes
	for addr := range activeNodes {
		if _, exists := c.conns[addr]; !exists {
			conn, err := newConnection(addr, c.config.DialOpts)
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

// route determines which connection to use for a given key
func (c *ClusterClient) route(key string) (*connection, error) {
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

// Put stores a value in the cache
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	req := &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds}
	_, err = conn.getClient().PutObject(ctx, req)
	conn.recordError(err)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
		topology, fetchErr := c.topology.FetchTopology(fetchCtx, c.seedAddrs, c.config.DialOpts)
		cancel()

		if fetchErr == nil {
			c.updateConnections(topology)
			if conn, routeErr := c.route(key); routeErr == nil {
				_, err = conn.getClient().PutObject(ctx, req)
				conn.recordError(err)
			}
		}
	}

	return err
}

// PutStream streams data to the cache
func (c *ClusterClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.getClient().Put(ctx)
	if err != nil {
		return err
	}

	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultBufferSize)
	defer release()

	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			req := &pb.PutRequest{Data: buf[:n]}
			if first {
				req.Key = key
				req.TtlSeconds = ttlSeconds
				first = false
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

// Get retrieves a value from the cache with retry logic
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	return c.getDataWithRetry(ctx, key, &pb.GetRequest{Key: key}, 1)
}

// getDataWithRetry implements Get/GetRange with retry logic
func (c *ClusterClient) getDataWithRetry(ctx context.Context, key string, req *pb.GetRequest, retryCount int) ([]byte, error) {
	conn, err := c.route(key)
	if err != nil {
		return nil, err
	}

	stream, err := conn.getClient().Get(ctx, req)
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
			// Retry on routing errors if we haven't received any data yet
			if isRoutingError(err) && retryCount > 0 && len(result) == 0 {
				fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
				topology, fetchErr := c.topology.FetchTopology(fetchCtx, c.seedAddrs, c.config.DialOpts)
				cancel()

				if fetchErr == nil {
					c.updateConnections(topology)
					return c.getDataWithRetry(ctx, key, req, retryCount-1)
				}
			}
			return nil, err
		}
		result = append(result, resp.Data...)
	}
	return result, nil
}

// GetStream streams a value from the cache
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	return c.getStreamWithRetry(ctx, key, &pb.GetRequest{Key: key}, w, 1)
}

// getStreamWithRetry implements GetStream/GetRangeStream with retry logic
func (c *ClusterClient) getStreamWithRetry(ctx context.Context, key string, req *pb.GetRequest, w io.Writer, retryCount int) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.getClient().Get(ctx, req)
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
			// Retry on routing errors if we haven't written any data yet
			if isRoutingError(err) && retryCount > 0 && bytesWritten == 0 {
				fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
				topology, fetchErr := c.topology.FetchTopology(fetchCtx, c.seedAddrs, c.config.DialOpts)
				cancel()

				if fetchErr == nil {
					c.updateConnections(topology)
					return c.getStreamWithRetry(ctx, key, req, w, retryCount-1)
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

// GetRange retrieves a byte range from the cache
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}
	return c.getDataWithRetry(ctx, key, req, 1)
}

// GetRangeStream streams a byte range from the cache
func (c *ClusterClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}
	return c.getStreamWithRetry(ctx, key, req, w, 1)
}

// Delete removes a key from the cache
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	_, err = conn.getClient().Delete(ctx, &pb.DeleteRequest{Key: key})

	// Retry once with topology refresh for cluster mode
	if isRoutingError(err) {
		fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
		topology, fetchErr := c.topology.FetchTopology(fetchCtx, c.seedAddrs, c.config.DialOpts)
		cancel()

		if fetchErr == nil {
			c.updateConnections(topology)
			if conn, routeErr := c.route(key); routeErr == nil {
				_, err = conn.getClient().Delete(ctx, &pb.DeleteRequest{Key: key})
			}
		}
	}

	return err
}

// List lists keys with optional prefix
func (c *ClusterClient) List(ctx context.Context, prefix string) ([]string, error) {
	// For list operation, use round-robin selection
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

	stream, err := conn.getClient().List(ctx, &pb.ListRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}

	var keys []string
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
			return nil, err
		}
		keys = append(keys, resp.Keys...)
	}
	return keys, nil
}

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
	return c.topology.FetchTopology(ctx, c.seedAddrs, c.config.DialOpts)
}

// UpdateTopology manually updates the topology (exposed for testing)
func (c *ClusterClient) UpdateTopology(topology *clusterpb.ClusterTopology) error {
	return c.updateConnections(topology)
}

// GetConnectionCount returns the number of active connections (exposed for testing)
func (c *ClusterClient) GetConnectionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns)
}
