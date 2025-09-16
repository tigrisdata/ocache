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

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// ClusterClient is a cluster-aware cache client that uses smart routing
// based on consistent hashing and partition ownership information
type ClusterClient struct {
	seedAddrs       []string
	seedClients     []*grpc.ClientConn         // Connections to seed nodes for topology
	clients         map[string]*Client         // nodeID -> client for all nodes
	ring            *consistent.Consistent     // Local hash ring for routing
	partitionOwners map[int32]string           // partition -> nodeID mapping
	topology        *clusterpb.ClusterTopology // Cached topology
	topologyEpoch   uint64                     // Current topology version
	currentIdx      int32                      // For round-robin fallback
	mu              sync.RWMutex
	stopCh          chan struct{}
}

// NewClusterClient creates a new cluster-aware client with smart routing
func NewClusterClient(seedAddrs []string, opts ...grpc.DialOption) (*ClusterClient, error) {
	if len(seedAddrs) == 0 {
		return nil, fmt.Errorf("at least one seed address is required")
	}

	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set max message sizes for streaming
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
	))

	c := &ClusterClient{
		seedAddrs:       seedAddrs,
		seedClients:     make([]*grpc.ClientConn, 0, len(seedAddrs)),
		clients:         make(map[string]*Client),
		partitionOwners: make(map[int32]string),
		stopCh:          make(chan struct{}),
	}

	// Connect to seed nodes
	for _, addr := range seedAddrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			// Log error but continue - we want to connect to as many as possible
			continue
		}
		c.seedClients = append(c.seedClients, conn)
	}

	if len(c.seedClients) == 0 {
		return nil, fmt.Errorf("failed to connect to any seed nodes")
	}

	// Fetch initial topology
	if err := c.refreshTopology(); err != nil {
		// Close connections and return error
		for _, conn := range c.seedClients {
			conn.Close()
		}
		return nil, fmt.Errorf("failed to fetch initial topology: %w", err)
	}

	// Start topology refresh loop
	go c.topologyRefreshLoop()

	return c, nil
}

// refreshTopology fetches the latest cluster topology from seed nodes
func (c *ClusterClient) refreshTopology() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try each seed node until we get topology
	for _, conn := range c.seedClients {
		client := clusterpb.NewClusterServiceClient(conn)
		topology, err := client.GetClusterTopology(ctx, &clusterpb.Empty{})
		if err != nil {
			// Try next seed
			continue
		}

		// Update topology
		return c.updateTopology(topology)
	}

	return fmt.Errorf("failed to get topology from any seed node")
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

	// Create/update clients for all nodes
	activeNodes := make(map[string]bool)
	for _, node := range topology.Nodes {
		activeNodes[node.Id] = true

		// Add to ring
		member := nodeMember(node.Id)
		ring.Add(member)

		// Create client if doesn't exist
		if _, exists := c.clients[node.Id]; !exists {
			client, err := New(node.Address)
			if err != nil {
				// Log error but continue
				continue
			}
			c.clients[node.Id] = client
		}
	}

	// Remove clients for nodes no longer in topology
	for nodeID, client := range c.clients {
		if !activeNodes[nodeID] {
			client.Close()
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
	ticker := time.NewTicker(30 * time.Second)
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

// getClientForKey returns the client for the node that owns the key
func (c *ClusterClient) getClientForKey(key string) (*Client, error) {
	// Find partition for key
	partition := c.getPartitionForKey(key)
	if partition < 0 {
		return nil, fmt.Errorf("no ring configured")
	}

	// Find node for partition
	nodeID := c.getNodeForPartition(partition)
	if nodeID == "" {
		return nil, fmt.Errorf("no owner for partition %d", partition)
	}

	// Get client for node
	c.mu.RLock()
	client, exists := c.clients[nodeID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no client for node %s", nodeID)
	}

	// Check if client is healthy
	if client.isHealthy() {
		return client, nil
	}

	return nil, fmt.Errorf("client for node %s is not healthy", nodeID)
}

// getRoundRobinClient returns a client using round-robin selection (fallback)
func (c *ClusterClient) getRoundRobinClient() (*Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.clients) == 0 {
		return nil, fmt.Errorf("no available clients")
	}

	// Convert map to slice for consistent ordering
	var nodeIDs []string
	for nodeID := range c.clients {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	// Try each client starting from current index
	startIdx := atomic.LoadInt32(&c.currentIdx)
	for i := 0; i < len(nodeIDs); i++ {
		idx := (int(startIdx) + i) % len(nodeIDs)
		nodeID := nodeIDs[idx]
		client := c.clients[nodeID]

		if client != nil && client.isHealthy() {
			// Update current index for next request
			atomic.StoreInt32(&c.currentIdx, int32((idx+1)%len(nodeIDs)))
			return client, nil
		}
	}

	return nil, fmt.Errorf("no healthy clients available")
}

// routeRequest routes a request with smart routing and fallback
func (c *ClusterClient) routeRequest(key string) (*Client, error) {
	// Try smart routing first
	client, err := c.getClientForKey(key)
	if err == nil {
		return client, nil
	}

	// Fallback to round-robin
	return c.getRoundRobinClient()
}

// Close closes all connections
func (c *ClusterClient) Close() error {
	close(c.stopCh)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Close seed connections
	for _, conn := range c.seedClients {
		conn.Close()
	}

	// Close node clients
	var lastErr error
	for _, client := range c.clients {
		if err := client.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Put stores a value in the cache using smart routing
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	client, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = client.Put(ctx, key, data, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return client.Put(ctx, key, data, ttlSeconds)
	}

	return err
}

// PutStream streams data to the cache using smart routing
func (c *ClusterClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	client, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = client.PutStream(ctx, key, r, ttlSeconds)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return client.PutStream(ctx, key, r, ttlSeconds)
	}

	return err
}

// Get retrieves a value from the cache using smart routing
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.routeRequest(key)
	if err != nil {
		return nil, err
	}

	data, err := client.Get(ctx, key)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return nil, err
		}
		return client.Get(ctx, key)
	}

	return data, err
}

// GetStream streams a value from the cache using smart routing
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	client, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = client.GetStream(ctx, key, w)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return client.GetStream(ctx, key, w)
	}

	return err
}

// GetRange retrieves a byte range from the cache using smart routing
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	client, err := c.routeRequest(key)
	if err != nil {
		return nil, err
	}

	data, err := client.GetRange(ctx, key, start, end)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return nil, err
		}
		return client.GetRange(ctx, key, start, end)
	}

	return data, err
}

// GetRangeStream streams a byte range from the cache using smart routing
func (c *ClusterClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	client, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = client.GetRangeStream(ctx, key, start, end, w)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return client.GetRangeStream(ctx, key, start, end, w)
	}

	return err
}

// Delete removes a key from the cache using smart routing
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	client, err := c.routeRequest(key)
	if err != nil {
		return err
	}

	err = client.Delete(ctx, key)

	// If we get a routing error, refresh topology and retry once
	if isRoutingError(err) {
		c.refreshTopology()
		client, err = c.routeRequest(key)
		if err != nil {
			return err
		}
		return client.Delete(ctx, key)
	}

	return err
}

// List lists all keys with optional prefix (uses round-robin)
func (c *ClusterClient) List(ctx context.Context, prefix string) ([]string, error) {
	client, err := c.getRoundRobinClient()
	if err != nil {
		return nil, err
	}
	return client.List(ctx, prefix)
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
