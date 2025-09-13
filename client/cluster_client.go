package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterClient is a cluster-aware cache client that routes requests
// to the appropriate node based on consistent hashing
type ClusterClient struct {
	nodes      map[string]*Client
	ring       *consistent.Consistent
	seedAddrs  []string
	mu         sync.RWMutex
	refreshing bool
	stopCh     chan struct{}
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// NewClusterClient creates a new cluster-aware client
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

	cfg := consistent.Config{
		PartitionCount:    16384,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}

	ring := consistent.New(nil, cfg)

	c := &ClusterClient{
		nodes:     make(map[string]*Client),
		ring:      ring,
		seedAddrs: seedAddrs,
		stopCh:    make(chan struct{}),
	}

	// Connect to seed nodes and discover cluster
	if err := c.discoverCluster(); err != nil {
		return nil, err
	}

	// Start background refresh of cluster state
	go c.refreshLoop()

	return c, nil
}

// discoverCluster connects to seed nodes and discovers all cluster members
func (c *ClusterClient) discoverCluster() error {
	for _, seed := range c.seedAddrs {
		if err := c.discoverFromSeed(seed); err != nil {
			// Try next seed
			continue
		}
		return nil // Successfully discovered cluster
	}
	return fmt.Errorf("failed to discover cluster from any seed node")
}

// discoverFromSeed discovers cluster members from a seed node
func (c *ClusterClient) discoverFromSeed(seedAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to seed %s: %w", seedAddr, err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)
	state, err := client.GetClusterState(ctx, &pb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state from %s: %w", seedAddr, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing ring
	c.ring = consistent.New(nil, consistent.Config{
		PartitionCount:    16384,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	})

	// Add all active nodes to ring and create clients
	for _, node := range state.Nodes {
		if node.Status != pb.NodeStatus_NODE_STATUS_ACTIVE {
			continue
		}

		// Add to ring
		member := nodeMember(node.Id)
		c.ring.Add(member)

		// Create client if not exists
		if _, exists := c.nodes[node.Id]; !exists {
			nodeClient, err := New(node.Address)
			if err != nil {
				// Log error but continue
				continue
			}
			c.nodes[node.Id] = nodeClient
		}
	}

	if len(c.nodes) == 0 {
		return fmt.Errorf("no active nodes found in cluster")
	}

	return nil
}

// refreshLoop periodically refreshes cluster state
func (c *ClusterClient) refreshLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.discoverCluster()
		case <-c.stopCh:
			return
		}
	}
}

// getClient returns the client for the node that owns the given key
func (c *ClusterClient) getClient(key string) (*Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	member := c.ring.LocateKey([]byte(key))
	if member == nil {
		return nil, fmt.Errorf("no node available for key %s", key)
	}

	client, exists := c.nodes[member.String()]
	if !exists {
		return nil, fmt.Errorf("client not found for node %s", member.String())
	}

	return client, nil
}

// Close closes all connections
func (c *ClusterClient) Close() error {
	close(c.stopCh)

	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for _, client := range c.nodes {
		if err := client.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Put stores a value in the cache
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	client, err := c.getClient(key)
	if err != nil {
		return err
	}
	return client.Put(ctx, key, data, ttlSeconds)
}

// PutStream streams data to the cache
func (c *ClusterClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	client, err := c.getClient(key)
	if err != nil {
		return err
	}
	return client.PutStream(ctx, key, r, ttlSeconds)
}

// Get retrieves a value from the cache
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.getClient(key)
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, key)
}

// GetStream streams a value from the cache
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	client, err := c.getClient(key)
	if err != nil {
		return err
	}
	return client.GetStream(ctx, key, w)
}

// GetRange retrieves a byte range from the cache
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	client, err := c.getClient(key)
	if err != nil {
		return nil, err
	}
	return client.GetRange(ctx, key, start, end)
}

// Delete removes a key from the cache
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	client, err := c.getClient(key)
	if err != nil {
		return err
	}
	return client.Delete(ctx, key)
}

// GetNodeForKey returns the node ID that owns the given key
func (c *ClusterClient) GetNodeForKey(key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	member := c.ring.LocateKey([]byte(key))
	if member == nil {
		return "", fmt.Errorf("no node available for key %s", key)
	}

	return member.String(), nil
}

// GetActiveNodes returns the list of active nodes in the cluster
func (c *ClusterClient) GetActiveNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}
