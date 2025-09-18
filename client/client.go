package cacheclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/tigrisdata/ocache/common/hash"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ConnectionMode defines how the client connects to servers
type ConnectionMode string

const (
	// ModeAuto automatically detects cluster vs simple mode (default)
	ModeAuto ConnectionMode = "auto"
	// ModeSimple uses direct connections without topology service
	ModeSimple ConnectionMode = "simple"
	// ModeCluster uses topology service for smart routing
	ModeCluster ConnectionMode = "cluster"
)

const (
	// DefaultRefreshInterval is the default topology refresh interval
	DefaultRefreshInterval = 30 * time.Second
	// MaxMessageSize is the maximum message size for gRPC
	MaxMessageSize = 128 * 1024 * 1024 // 128MB
	// TopologyDetectTimeout is the timeout for detecting cluster topology
	TopologyDetectTimeout = 2 * time.Second
)

// ClientConfig contains configuration for the unified Client
type ClientConfig struct {
	Addrs           []string          // One or more server addresses
	Mode            ConnectionMode    // Connection mode (default: "auto")
	RefreshInterval time.Duration     // Topology refresh for cluster mode (default: 30s)
	DialOpts        []grpc.DialOption // Optional gRPC dial options
}

// connection wraps a single gRPC connection
type connection struct {
	address string
	conn    *grpc.ClientConn
	client  pb.CacheServiceClient
}

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// Client is the unified cache client supporting both simple and cluster modes
type Client struct {
	conns map[string]*connection // address -> connection
	mode  ConnectionMode         // Actual mode (resolved from auto)

	// For cluster mode
	ring            *consistent.Consistent     // Consistent hash ring
	partitionOwners map[int32]string           // partition -> nodeID
	nodeAddresses   map[string]string          // nodeID -> address
	topology        *clusterpb.ClusterTopology // Current topology
	topologyEpoch   uint64                     // Topology version

	// For simple mode
	addresses  []string      // List of addresses
	currentIdx atomic.Uint32 // For round-robin

	config *ClientConfig
	mu     sync.RWMutex
	stopCh chan struct{}
}

// New creates a new client with default configuration
func New(addrs ...string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}
	return NewWithConfig(&ClientConfig{
		Addrs: addrs,
		Mode:  ModeAuto,
	})
}

// NewWithConfig creates a new client with custom configuration
func NewWithConfig(config *ClientConfig) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.Addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}

	// Set defaults
	if config.Mode == "" {
		config.Mode = ModeAuto
	}
	if config.RefreshInterval == 0 {
		config.RefreshInterval = DefaultRefreshInterval
	}
	if len(config.DialOpts) == 0 {
		config.DialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageSize),
				grpc.MaxCallSendMsgSize(MaxMessageSize),
			),
		}
	}

	client := &Client{
		conns:         make(map[string]*connection),
		nodeAddresses: make(map[string]string),
		config:        config,
		addresses:     config.Addrs,
		stopCh:        make(chan struct{}),
	}

	// Resolve auto mode
	if config.Mode == ModeAuto {
		client.mode = client.detectMode()
	} else {
		client.mode = config.Mode
	}

	// Initialize based on mode
	var err error
	switch client.mode {
	case ModeCluster:
		err = client.initClusterMode()
	case ModeSimple:
		err = client.initSimpleMode()
	default:
		err = fmt.Errorf("unknown mode: %s", client.mode)
	}

	if err != nil {
		// Clean up any created connections
		client.Close()
		return nil, err
	}

	return client, nil
}

// detectMode attempts to detect if cluster topology is available
func (c *Client) detectMode() ConnectionMode {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()

	// Try to fetch topology from any seed address
	for _, addr := range c.config.Addrs {
		conn, err := grpc.DialContext(ctx, addr, c.config.DialOpts...)
		if err != nil {
			continue
		}

		clusterClient := clusterpb.NewClusterServiceClient(conn)
		topology, err := clusterClient.GetClusterTopology(ctx, &clusterpb.Empty{})
		conn.Close()

		if err == nil && topology != nil {
			// Successfully fetched topology
			return ModeCluster
		}
	}

	// No topology service available, use simple mode
	return ModeSimple
}

// initSimpleMode initializes the client in simple mode
func (c *Client) initSimpleMode() error {
	// Create connections for each address
	for _, addr := range c.addresses {
		conn, err := c.createConnection(addr)
		if err != nil {
			return fmt.Errorf("failed to create connection for %s: %w", addr, err)
		}
		c.conns[addr] = conn
	}
	return nil
}

// initClusterMode initializes the client in cluster mode
func (c *Client) initClusterMode() error {
	// Fetch initial topology
	topology, err := c.fetchTopology()
	if err != nil {
		return fmt.Errorf("failed to fetch initial topology: %w", err)
	}

	// Initialize ring and pools based on topology
	if err := c.updateTopology(topology); err != nil {
		return fmt.Errorf("failed to initialize with topology: %w", err)
	}

	// Start topology refresh goroutine
	go c.topologyRefreshLoop()

	return nil
}

// createConnection creates a single connection to an address
func (c *Client) createConnection(addr string) (*connection, error) {
	conn, err := grpc.Dial(addr, c.config.DialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}
	return &connection{
		address: addr,
		conn:    conn,
		client:  pb.NewCacheServiceClient(conn),
	}, nil
}

// fetchTopology fetches the cluster topology from available nodes
func (c *Client) fetchTopology() (*clusterpb.ClusterTopology, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()

	// Try seed addresses first
	for _, addr := range c.config.Addrs {
		topology, err := c.fetchTopologyFromAddress(ctx, addr)
		if err == nil {
			return topology, nil
		}
	}

	// If we have existing topology, try those nodes
	c.mu.RLock()
	var nodeAddresses []string
	for _, addr := range c.nodeAddresses {
		nodeAddresses = append(nodeAddresses, addr)
	}
	c.mu.RUnlock()

	for _, addr := range nodeAddresses {
		topology, err := c.fetchTopologyFromAddress(ctx, addr)
		if err == nil {
			return topology, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch topology from any node")
}

// fetchTopologyFromAddress fetches topology from a specific address
func (c *Client) fetchTopologyFromAddress(ctx context.Context, addr string) (*clusterpb.ClusterTopology, error) {
	conn, err := grpc.DialContext(ctx, addr, c.config.DialOpts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)
	return client.GetClusterTopology(ctx, &clusterpb.Empty{})
}

// updateTopology updates the internal state based on new topology
func (c *Client) updateTopology(topology *clusterpb.ClusterTopology) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if topology has changed
	if c.topologyEpoch >= topology.Epoch {
		return nil // No change
	}

	// Create new ring
	cfg := consistent.Config{
		PartitionCount:    int(topology.RingConfig.PartitionCount),
		ReplicationFactor: int(topology.RingConfig.ReplicationFactor),
		Load:              topology.RingConfig.Load,
		Hasher:            hash.Hasher{},
	}
	ring := consistent.New(nil, cfg)

	// Build partition ownership map and node addresses
	partitionOwners := make(map[int32]string)
	nodeAddresses := make(map[string]string)
	activeNodes := make(map[string]bool)

	for _, node := range topology.Nodes {
		if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
			activeNodes[node.Id] = true
			nodeAddresses[node.Id] = node.Address
			ring.Add(nodeMember(node.Id))
		}
	}

	for _, owner := range topology.PartitionOwners {
		partitionOwners[owner.PartitionId] = owner.NodeId
	}

	// Create/update connections for all active nodes
	for _, address := range nodeAddresses {
		if _, exists := c.conns[address]; !exists {
			conn, err := c.createConnection(address)
			if err != nil {
				// Log error but continue
				continue
			}
			c.conns[address] = conn
		}
	}

	// Remove connections for nodes no longer in topology
	for addr, conn := range c.conns {
		found := false
		for _, nodeAddr := range nodeAddresses {
			if addr == nodeAddr {
				found = true
				break
			}
		}
		if !found {
			conn.close()
			delete(c.conns, addr)
		}
	}

	// Update state
	c.ring = ring
	c.partitionOwners = partitionOwners
	c.nodeAddresses = nodeAddresses
	c.topology = topology
	c.topologyEpoch = topology.Epoch

	return nil
}

// topologyRefreshLoop periodically refreshes the cluster topology
func (c *Client) topologyRefreshLoop() {
	ticker := time.NewTicker(c.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			topology, err := c.fetchTopology()
			if err == nil {
				c.updateTopology(topology)
			}
		case <-c.stopCh:
			return
		}
	}
}

// route determines which connection to use for a given key
func (c *Client) route(key string) (*connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch c.mode {
	case ModeCluster:
		return c.routeCluster(key)
	case ModeSimple:
		return c.routeSimple(key)
	default:
		return nil, fmt.Errorf("unknown mode: %s", c.mode)
	}
}

// routeSimple routes in simple mode (hash-based)
func (c *Client) routeSimple(key string) (*connection, error) {
	if len(c.conns) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// Use hash-based routing for better key locality
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	addr := c.addresses[hash%uint32(len(c.addresses))]
	conn, exists := c.conns[addr]
	if !exists {
		return nil, fmt.Errorf("no connection for address %s", addr)
	}
	return conn, nil
}

// routeCluster routes in cluster mode using consistent hashing
func (c *Client) routeCluster(key string) (*connection, error) {
	if c.ring == nil {
		return nil, fmt.Errorf("ring not initialized")
	}

	// Find partition for key
	partition := int32(c.ring.FindPartitionID([]byte(key)))

	// Get node for partition
	nodeID, exists := c.partitionOwners[partition]
	if !exists {
		return nil, fmt.Errorf("no owner for partition %d", partition)
	}

	// Get address for node
	addr, exists := c.nodeAddresses[nodeID]
	if !exists {
		return nil, fmt.Errorf("no address for node %s", nodeID)
	}

	// Get connection for address
	conn, exists := c.conns[addr]
	if !exists {
		return nil, fmt.Errorf("no connection for address %s", addr)
	}

	return conn, nil
}

// close closes the connection
func (c *connection) close() error {
	return c.conn.Close()
}

// Put stores a value in the cache
func (c *Client) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	req := &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds}
	_, err = conn.client.PutObject(ctx, req)

	// If we get a routing error in cluster mode, refresh topology and retry once
	if c.mode == ModeCluster && isRoutingError(err) {
		if topology, fetchErr := c.fetchTopology(); fetchErr == nil {
			c.updateTopology(topology)
			if conn, routeErr := c.route(key); routeErr == nil {
				_, err = conn.client.PutObject(ctx, req)
			}
		}
	}

	return err
}

// PutStream streams data to the cache
func (c *Client) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.client.Put(ctx)
	if err != nil {
		return err
	}

	buf := make([]byte, 64*1024) // 64KB chunks
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

// Get retrieves a value from the cache
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	req := &pb.GetRequest{Key: key}
	return c.getDataWithRetry(ctx, key, req, 1)
}

// getDataWithRetry implements Get/GetRange with retry logic
func (c *Client) getDataWithRetry(ctx context.Context, key string, req *pb.GetRequest, retryCount int) ([]byte, error) {
	conn, err := c.route(key)
	if err != nil {
		return nil, err
	}

	stream, err := conn.client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	var result []byte
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
			// 1. We're in cluster mode
			// 2. It's a routing error
			// 3. We haven't exceeded retry limit
			// 4. We haven't received any data yet (to avoid data loss)
			if c.mode == ModeCluster && isRoutingError(err) && retryCount > 0 && len(result) == 0 {
				if topology, fetchErr := c.fetchTopology(); fetchErr == nil {
					c.updateTopology(topology)
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
func (c *Client) GetStream(ctx context.Context, key string, w io.Writer) error {
	req := &pb.GetRequest{Key: key}
	return c.getStreamWithRetry(ctx, key, req, w, 1)
}

// getStreamWithRetry implements GetStream/GetRangeStream with retry logic
func (c *Client) getStreamWithRetry(ctx context.Context, key string, req *pb.GetRequest, w io.Writer, retryCount int) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.client.Get(ctx, req)
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
			// 1. We're in cluster mode
			// 2. It's a routing error
			// 3. We haven't exceeded retry limit
			// 4. We haven't written any data yet (to avoid duplicates)
			if c.mode == ModeCluster && isRoutingError(err) && retryCount > 0 && bytesWritten == 0 {
				if topology, fetchErr := c.fetchTopology(); fetchErr == nil {
					c.updateTopology(topology)
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
func (c *Client) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}
	return c.getDataWithRetry(ctx, key, req, 1)
}

// GetRangeStream streams a byte range from the cache
func (c *Client) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}
	return c.getStreamWithRetry(ctx, key, req, w, 1)
}

// Delete removes a key from the cache
func (c *Client) Delete(ctx context.Context, key string) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	_, err = conn.client.Delete(ctx, &pb.DeleteRequest{Key: key})

	// Retry once with topology refresh for cluster mode
	if c.mode == ModeCluster && isRoutingError(err) {
		if topology, fetchErr := c.fetchTopology(); fetchErr == nil {
			c.updateTopology(topology)
			if conn, routeErr := c.route(key); routeErr == nil {
				_, err = conn.client.Delete(ctx, &pb.DeleteRequest{Key: key})
			}
		}
	}

	return err
}

// List lists keys with optional prefix
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	// For list operation, just use round-robin selection
	var conn *connection

	c.mu.RLock()
	if len(c.conns) > 0 {
		// Get all connections and sort for consistent ordering
		var addresses []string
		for addr := range c.conns {
			addresses = append(addresses, addr)
		}
		sort.Strings(addresses)

		idx := c.currentIdx.Add(1) - 1
		addr := addresses[idx%uint32(len(addresses))]
		conn = c.conns[addr]
	}
	c.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("no available connections")
	}

	stream, err := conn.client.List(ctx, &pb.ListRequest{Prefix: prefix})
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
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already closed
	select {
	case <-c.stopCh:
		// Already closed
		return nil
	default:
		// Signal stop to background goroutines
		close(c.stopCh)
	}

	// Close all connections
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.conns = make(map[string]*connection)
	return firstErr
}

// GetMode returns the actual connection mode being used
func (c *Client) GetMode() ConnectionMode {
	return c.mode
}

// GetConnectedNodes returns the addresses of all connected nodes
func (c *Client) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var nodes []string
	for addr := range c.conns {
		nodes = append(nodes, addr)
	}
	sort.Strings(nodes)
	return nodes
}

// GetTopologyEpoch returns the current topology epoch (cluster mode only)
func (c *Client) GetTopologyEpoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topologyEpoch
}

// HasRing returns true if the consistent hash ring is initialized (cluster mode only)
func (c *Client) HasRing() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring != nil
}

// GetPartitionOwner returns the node ID that owns the given partition (cluster mode only)
func (c *Client) GetPartitionOwner(partitionID int32) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.partitionOwners[partitionID]
}

// GetPartitionOwnerCount returns the number of partition owners (cluster mode only)
func (c *Client) GetPartitionOwnerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.partitionOwners)
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
