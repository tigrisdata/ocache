package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/tigrisdata/ocache/common/hash"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// DefaultPoolSizePerNode is the default pool size per node
	DefaultPoolSizePerNode = 4
	// DefaultTopologyRefreshInterval is the default interval for refreshing the cluster topology
	DefaultTopologyRefreshInterval = 30 * time.Second
)

// ClusterClientConfig contains configuration for ClusterClient
type ClusterClientConfig struct {
	SeedAddrs               []string
	PoolSizePerNode         int           // Default: 4
	TopologyRefreshInterval time.Duration // Default: 30s
	DialOpts                []grpc.DialOption
}

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// ClusterClient is a cluster-aware cache client that uses smart routing
// based on consistent hashing and partition ownership information
type ClusterClient struct {
	clients         map[string]*ConnectionPool // nodeID -> connection pool for all nodes
	ring            *consistent.Consistent     // Local hash ring for routing
	partitionOwners map[int32]string           // partition -> nodeID mapping
	topology        *clusterpb.ClusterTopology // Cached topology
	topologyEpoch   uint64                     // Current topology version
	currentIdx      int32                      // For round-robin fallback
	config          *ClusterClientConfig       // Store config
	mu              sync.RWMutex
	stopCh          chan struct{}
}

// NewClusterClient creates a new cluster-aware client with smart routing
func NewClusterClient(config *ClusterClientConfig) (*ClusterClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.SeedAddrs) == 0 {
		return nil, fmt.Errorf("at least one seed address is required")
	}

	// Set defaults
	if config.PoolSizePerNode <= 0 {
		config.PoolSizePerNode = DefaultPoolSizePerNode
	}
	if config.TopologyRefreshInterval == 0 {
		config.TopologyRefreshInterval = DefaultTopologyRefreshInterval
	}
	if len(config.DialOpts) == 0 {
		config.DialOpts = append(config.DialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	c := &ClusterClient{
		clients:         make(map[string]*ConnectionPool),
		partitionOwners: make(map[int32]string),
		config:          config,
		stopCh:          make(chan struct{}),
	}

	// Fetch initial topology from seed nodes
	topology, err := fetchTopologyFromNodes(config.SeedAddrs, config.DialOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch initial topology: %w", err)
	}

	// Initialize with topology
	if err := c.updateTopology(topology); err != nil {
		return nil, fmt.Errorf("failed to initialize with topology: %w", err)
	}

	// Start topology refresh loop
	go c.topologyRefreshLoop()

	return c, nil
}

// fetchTopologyFromNodes connects to nodes to fetch topology
func fetchTopologyFromNodes(nodeAddrs []string, opts []grpc.DialOption) (*clusterpb.ClusterTopology, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try each node until we get topology
	for _, addr := range nodeAddrs {
		conn, err := grpc.DialContext(ctx, addr, opts...)
		if err != nil {
			continue // Try next node
		}
		defer conn.Close()

		client := clusterpb.NewClusterServiceClient(conn)
		topology, err := client.GetClusterTopology(ctx, &clusterpb.Empty{})
		if err != nil {
			continue // Try next node
		}

		return topology, nil
	}

	return nil, fmt.Errorf("failed to get topology from any node")
}

// refreshTopology fetches the latest cluster topology from existing nodes
func (c *ClusterClient) refreshTopology() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get current nodes from topology
	c.mu.RLock()
	var nodeAddresses []string
	if c.topology != nil {
		for _, node := range c.topology.Nodes {
			if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
				nodeAddresses = append(nodeAddresses, node.Address)
			}
		}
	}
	c.mu.RUnlock()

	// If no nodes in topology, we can't refresh
	if len(nodeAddresses) == 0 {
		return fmt.Errorf("no active nodes available for topology refresh")
	}

	// Try each node until we get topology
	for _, addr := range nodeAddresses {
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(128*1024*1024),
				grpc.MaxCallSendMsgSize(128*1024*1024),
			))
		if err != nil {
			continue // Try next node
		}
		defer conn.Close()

		client := clusterpb.NewClusterServiceClient(conn)
		topology, err := client.GetClusterTopology(ctx, &clusterpb.Empty{})
		if err != nil {
			continue // Try next node
		}

		// Update topology
		return c.updateTopology(topology)
	}

	return fmt.Errorf("failed to get topology from any node")
}

// updateTopology updates the local ring and partition ownership based on new topology
func (c *ClusterClient) updateTopology(topology *clusterpb.ClusterTopology) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if topology has changed
	if c.topologyEpoch >= topology.Epoch {
		return nil // No change
	}

	// Create new ring with same configuration as server
	cfg := consistent.Config{
		PartitionCount:    int(topology.RingConfig.PartitionCount),
		ReplicationFactor: int(topology.RingConfig.ReplicationFactor),
		Load:              topology.RingConfig.Load,
		Hasher:            hash.Hasher{},
	}
	ring := consistent.New(nil, cfg)

	// Build partition ownership map
	partitionOwners := make(map[int32]string)
	for _, owner := range topology.PartitionOwners {
		partitionOwners[owner.PartitionId] = owner.NodeId
	}

	// Create/update pools for all nodes
	activeNodes := make(map[string]bool)
	for _, node := range topology.Nodes {
		activeNodes[node.Id] = true

		// Add to ring
		member := nodeMember(node.Id)
		ring.Add(member)

		// Create pool if doesn't exist
		if _, exists := c.clients[node.Id]; !exists {
			pool, err := NewConnectionPool(node.Address, c.config.PoolSizePerNode, c.config.DialOpts...)
			if err != nil {
				// Log error but continue
				continue
			}
			c.clients[node.Id] = pool
		}
	}

	// Remove pools for nodes no longer in topology
	for nodeID, pool := range c.clients {
		if !activeNodes[nodeID] {
			pool.Close()
			delete(c.clients, nodeID)
		}
	}

	// Update state
	c.ring = ring
	c.partitionOwners = partitionOwners
	c.topology = topology
	c.topologyEpoch = topology.Epoch

	return nil
}

// topologyRefreshLoop periodically refreshes cluster topology
func (c *ClusterClient) topologyRefreshLoop() {
	ticker := time.NewTicker(c.config.TopologyRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.refreshTopology()
		case <-c.stopCh:
			return
		}
	}
}

// getPartitionForKey returns the partition ID for a given key
func (c *ClusterClient) getPartitionForKey(key string) int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ring == nil {
		return -1
	}

	return int32(c.ring.FindPartitionID([]byte(key)))
}

// getNodeForPartition returns the node ID that owns the given partition
func (c *ClusterClient) getNodeForPartition(partition int32) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.partitionOwners[partition]
}

// getPoolForKey returns the connection pool for the node that owns the key
func (c *ClusterClient) getPoolForKey(key string) (*ConnectionPool, error) {
	// Find node for key
	nodeID, err := c.GetNodeForKey(key)
	if err != nil {
		return nil, err
	}

	// Get pool for node
	c.mu.RLock()
	pool, exists := c.clients[nodeID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no connection pool for node %s", nodeID)
	}

	return pool, nil
}

// getRoundRobinPool returns a connection pool using round-robin selection (fallback)
func (c *ClusterClient) getRoundRobinPool() (*ConnectionPool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.clients) == 0 {
		return nil, fmt.Errorf("no available connection pools")
	}

	// Convert map to slice for consistent ordering
	var nodeIDs []string
	for nodeID := range c.clients {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	// Select next pool using round-robin
	startIdx := atomic.LoadInt32(&c.currentIdx)
	idx := int(startIdx) % len(nodeIDs)
	nodeID := nodeIDs[idx]
	pool := c.clients[nodeID]

	// Update current index for next request
	atomic.StoreInt32(&c.currentIdx, int32((idx+1)%len(nodeIDs)))

	return pool, nil
}

// routeRequest routes a request with smart routing and fallback
func (c *ClusterClient) routeRequest(key string) (*ConnectionPool, error) {
	// Try smart routing first
	pool, err := c.getPoolForKey(key)
	if err == nil {
		return pool, nil
	}

	// Fallback to round-robin
	return c.getRoundRobinPool()
}

// Close closes all connections
func (c *ClusterClient) Close() error {
	close(c.stopCh)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Close all connection pools
	var lastErr error
	for _, pool := range c.clients {
		if err := pool.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Put stores a value in the cache using smart routing
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	pool, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = pool.Put(ctx, key, data, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return pool.Put(ctx, key, data, ttlSeconds)
	}

	return err
}

// PutStream streams data to the cache using smart routing
func (c *ClusterClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	pool, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = pool.PutStream(ctx, key, r, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return pool.PutStream(ctx, key, r, ttlSeconds)
	}

	return err
}

// Get retrieves a value from the cache using smart routing
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	pool, err := c.routeRequest(key)
	if err != nil {
		return nil, err
	}

	data, err := pool.Get(ctx, key)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return nil, err
		}
		return pool.Get(ctx, key)
	}

	return data, err
}

// GetStream streams a value from the cache using smart routing
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	pool, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = pool.GetStream(ctx, key, w)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return pool.GetStream(ctx, key, w)
	}

	return err
}

// GetRange retrieves a byte range from the cache using smart routing
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	pool, err := c.routeRequest(key)
	if err != nil {
		return nil, err
	}

	data, err := pool.GetRange(ctx, key, start, end)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return nil, err
		}
		return pool.GetRange(ctx, key, start, end)
	}

	return data, err
}

// GetRangeStream streams a byte range from the cache using smart routing
func (c *ClusterClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	pool, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = pool.GetRangeStream(ctx, key, start, end, w)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return pool.GetRangeStream(ctx, key, start, end, w)
	}

	return err
}

// Delete removes a key from the cache using smart routing
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	pool, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = pool.Delete(ctx, key)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		pool, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return pool.Delete(ctx, key)
	}

	return err
}

// List lists all keys with optional prefix (uses round-robin)
func (c *ClusterClient) List(ctx context.Context, prefix string) ([]string, error) {
	pool, err := c.getRoundRobinPool()
	if err != nil {
		return nil, err
	}
	return pool.List(ctx, prefix)
}

// GetNodeForKey returns the node ID that owns the given key
func (c *ClusterClient) GetNodeForKey(key string) (string, error) {
	partition := c.getPartitionForKey(key)
	if partition < 0 {
		return "", fmt.Errorf("no ring configured")
	}

	nodeID := c.getNodeForPartition(partition)
	if nodeID == "" {
		return "", fmt.Errorf("no owner for partition %d", partition)
	}

	return nodeID, nil
}

// GetConnectedNodes returns the IDs of all connected nodes
func (c *ClusterClient) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.clients))
	for nodeID := range c.clients {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// isRoutingError checks if an error indicates we should refresh topology
func isRoutingError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	// These errors indicate the node doesn't own the key anymore
	return st.Code() == codes.FailedPrecondition ||
		st.Code() == codes.NotFound ||
		st.Code() == codes.Unavailable
}
