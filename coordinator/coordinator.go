package coordinator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/hash"
	"github.com/tigrisdata/ocache/coordinator/discovery"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// MaxMessageSize is the maximum message size for gRPC messages
	MaxMessageSize = 128 * 1024 * 1024 // 128MB

	// HeartbeatRequestTimeout is the timeout for heartbeat requests
	HeartbeatRequestTimeout = 2 * time.Second

	// FailureDetectionInterval is the interval at which failure detection is performed
	FailureDetectionInterval = 10 * time.Second

	// HeartbeatInterval is the interval at which heartbeats are sent to detect node failures
	DefaultHeartbeatInterval = 5 * time.Second

	// FailureThreshold is the number of consecutive failures before a node is marked as down
	DefaultFailureThreshold = 3

	// DefaultDNSRefreshInterval is the default interval for DNS refresh
	DefaultDNSRefreshInterval = 30 * time.Second

	// DefaultBroadcastCacheTime is the default time for broadcasting cache state
	DefaultBroadcastCacheTime = 10 * time.Second

	// DefaultSyncTimeout is the default timeout for syncing with other nodes
	DefaultSyncTimeout = 10 * time.Second
)

// Config contains the configuration for the coordinator
type Config struct {
	Enabled            bool          // Whether the coordinator is enabled
	MyNodeID           string        // The ID of the node
	ClusterAddr        string        // The address the coordinator will listen on for cluster communication
	ListenAddr         string        // The address the node listens on for client requests (Put/Get/Delete)
	Nodes              []string      // The nodes of the cluster (can be static list or DNS name)
	RingPartitionCount int           // The number of partitions in the hash ring
	HeartbeatInterval  int           // The interval at which heartbeats are sent to detect node failures
	FailureThreshold   int           // The number of consecutive failures before a node is marked as down
	AllowLocalhost     bool          // Whether to restrict to localhost addresses (for testing)
	DNSRefreshInterval int           // The interval at which DNS node discovery is performed (seconds)
	SyncTimeout        int           // The timeout for syncing with other nodes (seconds)
	RouterConfig       *RouterConfig // Configuration for the Router (optional, uses defaults if nil)
}

// Coordinator manages cluster membership, request routing, and cluster RPC handling.
// Combines membership tracking, failure detection, and cluster RPC handling.
type Coordinator struct {
	clusterpb.UnimplementedClusterServiceServer

	// Configuration for the coordinator
	config *Config

	// Node discovery
	nodeDiscovery discovery.NodeDiscovery
	currentNodes  []string     // Currently resolved nodes
	nodesMu       sync.RWMutex // Protects currentNodes

	// Core components
	ring   *Ring
	router *Router

	// Membership tracking
	heartbeatInterval time.Duration
	failureThreshold  int
	failureCount      map[string]int
	lastHeartbeat     map[string]time.Time // Last heartbeat time for each node
	syncTimeout       time.Duration
	mu                sync.RWMutex

	// Broadcast deduplication
	broadcastCache   map[string]time.Time // Track recent broadcasts to prevent loops
	broadcastCacheMu sync.RWMutex

	// Lifecycle management
	grpcServer *grpc.Server
	stopCh     chan struct{}
	errCh      chan error // Channel for propagating fatal errors
	wg         sync.WaitGroup
}

// New creates a new coordinator
func New(config *Config) (*Coordinator, error) {
	if !config.Enabled {
		zlog.Debug().Msg("Coordinator disabled, returning nil")
		return nil, nil
	}

	zlog.Debug().
		Str("node_id", config.MyNodeID).
		Str("cluster_addr", config.ClusterAddr).
		Int("node_count", len(config.Nodes)).
		Int("partition_count", config.RingPartitionCount).
		Msg("Creating new coordinator")

	if config.MyNodeID == "" {
		return nil, fmt.Errorf("node ID is required in cluster mode")
	}

	// Validate cluster address
	if err := discovery.ValidateClusterAddress(config.ClusterAddr); err != nil {
		return nil, fmt.Errorf("invalid cluster address: %w", err)
	}

	// Validate listen address is provided
	if config.ListenAddr == "" {
		return nil, fmt.Errorf("listen address is required in cluster mode")
	}

	// Create node discovery
	dnsRefreshInterval := time.Duration(config.DNSRefreshInterval) * time.Second
	if dnsRefreshInterval <= 0 {
		dnsRefreshInterval = discovery.DefaultDNSRefreshInterval
	}

	nodeDiscovery, err := discovery.CreateNodeDiscovery(config.Nodes, dnsRefreshInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create node discovery: %w", err)
	}

	// Initial nodes resolution
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initialNodes, err := nodeDiscovery.Resolve(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve initial nodes: %w", err)
	}

	zlog.Info().
		Str("discovery_mode", string(nodeDiscovery.Mode())).
		Int("resolved_nodes", len(initialNodes)).
		Strs("nodes", initialNodes).
		Str("discovery", nodeDiscovery.String()).
		Msg("Node discovery initialized")

	// Default to 16384 partitions if not set
	if config.RingPartitionCount < 1 {
		config.RingPartitionCount = hash.DefaultPartitionCount
	}

	ring, err := NewRing(config.RingPartitionCount, config.MyNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring: %w", err)
	}

	// Create router with config if provided, otherwise use defaults
	var router *Router
	if config.RouterConfig != nil {
		router = NewRouterWithConfig(ring, config.MyNodeID, config.RouterConfig)
	} else {
		router = NewRouter(ring, config.MyNodeID)
	}

	heartbeatInterval := time.Duration(config.HeartbeatInterval) * time.Second
	if heartbeatInterval == 0 {
		heartbeatInterval = DefaultHeartbeatInterval
	}

	failureThreshold := config.FailureThreshold
	if failureThreshold == 0 {
		failureThreshold = DefaultFailureThreshold
	}

	syncTimeout := time.Duration(config.SyncTimeout) * time.Second
	if syncTimeout == 0 {
		syncTimeout = DefaultSyncTimeout
	}

	coord := &Coordinator{
		config:            config,
		nodeDiscovery:     nodeDiscovery,
		currentNodes:      initialNodes,
		ring:              ring,
		router:            router,
		heartbeatInterval: heartbeatInterval,
		failureThreshold:  failureThreshold,
		syncTimeout:       syncTimeout,
		failureCount:      make(map[string]int),
		lastHeartbeat:     make(map[string]time.Time),
		broadcastCache:    make(map[string]time.Time),
		stopCh:            make(chan struct{}),
		errCh:             make(chan error, 1), // Buffered to prevent blocking
	}

	// Add self to ring immediately during initialization
	if _, err := ring.AddNode(config.MyNodeID, config.ClusterAddr, config.ListenAddr); err != nil {
		return nil, fmt.Errorf("failed to add self to ring: %w", err)
	}

	return coord, nil
}

// Start starts the coordinator and joins the cluster
func (c *Coordinator) Start(ctx context.Context) error {
	// Start gRPC server for cluster communication
	// It will be used to communicate with other nodes in the cluster
	lis, err := net.Listen("tcp", c.config.ClusterAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on cluster address %s: %w", c.config.ClusterAddr, err)
	}

	c.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxMessageSize),
		grpc.MaxSendMsgSize(MaxMessageSize),
	)

	clusterpb.RegisterClusterServiceServer(c.grpcServer, c)

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Str("listen_addr", c.config.ClusterAddr).
		Msg("Starting cluster gRPC server")

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.grpcServer.Serve(lis); err != nil {
			zlog.Error().Err(err).Msg("Cluster gRPC server failed")
			// Propagate fatal error unless we're shutting down gracefully
			select {
			case <-ctx.Done():
				// Context cancelled, graceful shutdown
			case c.errCh <- fmt.Errorf("coordinator gRPC server failed: %w", err):
				// Error sent
			default:
				// Channel full or closed, log and continue
			}
		}
	}()

	// Join cluster
	zlog.Debug().Msg("Attempting to join cluster")
	if err := c.joinCluster(); err != nil {
		// Cleanup: stop the gRPC server we started before returning error
		if c.grpcServer != nil {
			c.grpcServer.Stop() // Use Stop() not GracefulStop() since we're in error state
		}
		c.wg.Wait() // Wait for serve goroutine to exit
		return fmt.Errorf("failed to join cluster: %w", err)
	}

	// Start background tasks
	zlog.Debug().
		Bool("node_refresh", c.nodeDiscovery.NeedsRefresh()).
		Msg("Starting background tasks")

	c.wg.Add(2)
	go c.sendHeartbeatsLoop()
	go c.failureDetectionLoop()

	if c.nodeDiscovery.NeedsRefresh() {
		c.wg.Add(1)
		go c.nodeDiscoveryLoop()
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("cluster_addr", c.config.ClusterAddr).
		Int("partition_count", c.config.RingPartitionCount).
		Int("node_count", len(c.config.Nodes)).
		Msg("Coordinator started")

	return nil
}

// Stop stops the coordinator and cleans up resources
func (c *Coordinator) Stop() error {
	close(c.stopCh)

	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}

	if err := c.router.Close(); err != nil {
		zlog.Error().Err(err).Msg("Error closing router connections")
	}

	c.wg.Wait()

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Coordinator stopped successfully")

	return nil
}

// joinCluster bootstraps or joins existing cluster via current discovered nodes
func (c *Coordinator) joinCluster() error {
	// Self is already added to ring in New()

	// Get current nodes
	c.nodesMu.RLock()
	nodes := c.currentNodes
	c.nodesMu.RUnlock()

	// Filter out self from the seed list to avoid attempting to sync with ourselves
	otherNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != c.config.ClusterAddr {
			otherNodes = append(otherNodes, node)
		}
	}

	// If no other nodes after filtering, we're the bootstrap node
	if len(otherNodes) == 0 {
		zlog.Info().
			Str("node_id", c.config.MyNodeID).
			Msg("Starting as bootstrap node (no other seeds available)")
		return nil
	}

	// Try to sync with all the nodes to get the cluster state
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Strs("nodes", otherNodes).
		Str("discovery_mode", string(c.nodeDiscovery.Mode())).
		Msg("Attempting to sync with nodes")

	var lastErr error
	for _, node := range otherNodes {
		if err := c.syncWithNode(node); err != nil {
			zlog.Warn().
				Err(err).
				Str("node", node).
				Msg("Failed to sync with node")
			lastErr = err
			continue
		}
		// Successfully synced with at least one node
		zlog.Info().
			Str("node_id", c.config.MyNodeID).
			Str("node", node).
			Msg("Successfully synced with node")
		return nil
	}

	// All node sync attempts failed - this is acceptable if we're the first node starting up
	// Fall back to bootstrap mode
	zlog.Warn().
		Err(lastErr).
		Str("node_id", c.config.MyNodeID).
		Int("attempted_seeds", len(otherNodes)).
		Msg("Failed to sync with any seed node, starting as bootstrap node")
	return nil
}

// syncWithNode gets cluster state from node and announces our join to the cluster
func (c *Coordinator) syncWithNode(nodeAddr string) error {
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Str("node", nodeAddr).
		Msg("Syncing with node")

	ctx, cancel := context.WithTimeout(context.Background(), c.syncTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	// Get current cluster state from the node
	zlog.Debug().
		Str("node", nodeAddr).
		Msg("Requesting cluster state from node")

	state, err := client.GetClusterState(ctx, &clusterpb.Empty{})
	if err != nil {
		return err
	}

	zlog.Debug().
		Str("node", nodeAddr).
		Int("node_count", len(state.Nodes)).
		Uint64("epoch", state.Epoch).
		Msg("Received cluster state from node")

	// Add all nodes to our ring to make sure we have the correct state
	for _, node := range state.Nodes {
		// Skip self
		if node.Id == c.config.MyNodeID {
			continue
		}

		// Add node to our ring
		if _, err := c.ring.AddNode(node.Id, node.Address, node.ListenAddress); err != nil {
			zlog.Warn().
				Err(err).
				Str("node_id", node.Id).
				Msg("Failed to add node to ring")
		}
	}

	// Announce our join to the cluster
	joinReq := &clusterpb.JoinRequest{
		NodeId:        c.config.MyNodeID,
		Address:       c.config.ClusterAddr,
		ListenAddress: c.config.ListenAddr,
	}

	// Send join request to the node
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Str("node", nodeAddr).
		Msg("Sending join request to node")

	_, err = client.Join(ctx, joinReq)
	if err != nil {
		return err
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("node", nodeAddr).
		Int("cluster_size", len(state.Nodes)+1).
		Msg("Successfully joined cluster")

	return nil
}

// sendHeartbeatsLoop periodically sends heartbeats to detect node failures
func (c *Coordinator) sendHeartbeatsLoop() {
	defer c.wg.Done()

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Dur("interval", c.heartbeatInterval).
		Msg("Starting heartbeat loop")

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sendHeartbeats()
		case <-c.stopCh:
			zlog.Debug().
				Str("node_id", c.config.MyNodeID).
				Msg("Heartbeat loop stopping")
			return
		}
	}
}

// sendHeartbeats sends heartbeats to detect node failures
func (c *Coordinator) sendHeartbeats() {
	// Send heartbeats to all active nodes
	nodes := c.ring.GetActiveNodes()

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Int("active_nodes", len(nodes)).
		Msg("Sending heartbeats to nodes")
	for _, node := range nodes {
		if node.ID == c.config.MyNodeID {
			continue
		}

		go func(n *NodeInfo) {
			ctx, cancel := context.WithTimeout(context.Background(), HeartbeatRequestTimeout)
			defer cancel()

			conn, err := grpc.DialContext(ctx, n.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				zlog.Warn().
					Err(err).
					Str("node_id", n.ID).
					Msg("Failed to send heartbeat to node")

				// Update the node failure count on connection failure
				c.recordFailure(n.ID)
				return
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)
			req := &clusterpb.HeartbeatRequest{
				NodeId: c.config.MyNodeID,
				Epoch:  c.ring.GetEpoch(),
			}

			resp, err := client.Heartbeat(ctx, req)
			if err != nil {
				zlog.Warn().
					Err(err).
					Str("node_id", n.ID).
					Msg("Failed to send heartbeat to node")

				// Update the node failure count on heartbeat request failure
				c.recordFailure(n.ID)
			} else {
				// Check if heartbeat epoch is newer
				if resp.Epoch > c.ring.GetEpoch() {
					zlog.Info().
						Str("node_id", n.ID).
						Uint64("local_epoch", c.ring.GetEpoch()).
						Uint64("remote_epoch", resp.Epoch).
						Msg("Received heartbeat with newer membership epoch, may need to sync")
				}

				zlog.Debug().
					Str("node_id", n.ID).
					Msg("Successfully sent heartbeat to node")

				// Reset the node failure count on heartbeat success
				c.recordSuccess(n.ID)
			}
		}(node)
	}
}

// recordFailure tracks heartbeat failures, marks node down after threshold
func (c *Coordinator) recordFailure(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failureCount[nodeID]++

	// Mark node as down if failure threshold is exceeded
	if c.failureCount[nodeID] >= c.failureThreshold {
		zlog.Warn().
			Str("node_id", nodeID).
			Int("failures", c.failureCount[nodeID]).
			Msg("Node exceeded failure threshold, marking as down")

		if err := c.ring.UpdateNodeStatus(nodeID, NodeStatusDown); err != nil {
			zlog.Error().
				Err(err).
				Str("node_id", nodeID).
				Msg("Failed to update node status")
		}
	}
}

// recordSuccess resets failure count on successful heartbeat
func (c *Coordinator) recordSuccess(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failureCount[nodeID] = 0
	c.lastHeartbeat[nodeID] = time.Now()
}

// failureDetectionLoop checks for nodes that haven't sent heartbeats
func (c *Coordinator) failureDetectionLoop() {
	defer c.wg.Done()

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Starting failure detection loop")

	ticker := time.NewTicker(FailureDetectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.verifyLastHeartbeat()
		case <-c.stopCh:
			zlog.Debug().
				Str("node_id", c.config.MyNodeID).
				Msg("Failure detection loop stopping")
			return
		}
	}
}

// verifyLastHeartbeat marks nodes as down if heartbeat timeout exceeded
func (c *Coordinator) verifyLastHeartbeat() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.lastHeartbeat) > 0 {
		zlog.Debug().
			Str("node_id", c.config.MyNodeID).
			Int("tracked_nodes", len(c.lastHeartbeat)).
			Msg("Checking for failed nodes")
	}

	// Calculate timeout as a multiple of the heartbeat interval
	timeout := time.Duration(c.failureThreshold) * c.heartbeatInterval
	now := time.Now()

	for nodeID, lastSeen := range c.lastHeartbeat {
		// Skip checking our own heartbeat
		if nodeID == c.config.MyNodeID {
			continue
		}

		// Check if heartbeat timeout exceeded
		if now.Sub(lastSeen) > timeout {
			// Check current node status to avoid redundant updates
			status, err := c.ring.GetNodeStatus(nodeID)
			if err != nil {
				zlog.Debug().
					Err(err).
					Str("node_id", nodeID).
					Msg("Failed to get node status, skipping heartbeat timeout check")
				continue
			}

			// Skip if already marked as down
			if status == NodeStatusDown {
				continue
			}

			zlog.Warn().
				Str("node_id", nodeID).
				Dur("last_seen", now.Sub(lastSeen)).
				Msg("Node heartbeat timeout, marking as down")

			// Mark node as down because heartbeat timeout exceeded
			if err := c.ring.UpdateNodeStatus(nodeID, NodeStatusDown); err != nil {
				zlog.Error().
					Err(err).
					Str("node_id", nodeID).
					Msg("Failed to update node status")
			}
		}
	}
}

// nodeDiscoveryLoop periodically refreshes node addresses if needed
func (c *Coordinator) nodeDiscoveryLoop() {
	defer c.wg.Done()

	// Only run if discovery needs refresh (should be checked but double-check)
	if !c.nodeDiscovery.NeedsRefresh() {
		return
	}

	interval := c.nodeDiscovery.RefreshInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("discovery_mode", string(c.nodeDiscovery.Mode())).
		Dur("refresh_interval", interval).
		Msg("Starting node discovery refresh loop")

	for {
		select {
		case <-ticker.C:
			c.refreshNodes()
		case <-c.stopCh:
			zlog.Debug().
				Str("node_id", c.config.MyNodeID).
				Msg("Node discovery loop stopping")
			return
		}
	}
}

// refreshNodes re-resolves nodes and updates the cluster membership
func (c *Coordinator) refreshNodes() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	newNodes, err := c.nodeDiscovery.Resolve(ctx)
	if err != nil {
		zlog.Warn().
			Err(err).
			Str("discovery", c.nodeDiscovery.String()).
			Msg("Failed to refresh node addresses")
		return
	}

	c.nodesMu.RLock()
	oldNodes := make([]string, len(c.currentNodes))
	copy(oldNodes, c.currentNodes)
	c.nodesMu.RUnlock()

	// Compare old and new nodes
	added, removed := discovery.DiffNodes(oldNodes, newNodes)

	if len(added) > 0 || len(removed) > 0 {
		zlog.Info().
			Strs("added", added).
			Strs("removed", removed).
			Int("old_count", len(oldNodes)).
			Int("new_count", len(newNodes)).
			Msg("Node addresses changed")

		// Update current nodes
		c.nodesMu.Lock()
		c.currentNodes = newNodes
		c.nodesMu.Unlock()

		// Try to sync with new nodes
		for _, node := range added {
			go c.tryJoinNode(node)
		}
	} else {
		zlog.Debug().
			Int("node_count", len(newNodes)).
			Msg("Node addresses unchanged")
	}
}

// tryJoinNode attempts to sync with a newly discovered node
func (c *Coordinator) tryJoinNode(node string) {
	// Skip if it's our own address
	if node == c.config.ClusterAddr {
		return
	}

	zlog.Info().
		Str("node", node).
		Msg("Attempting to sync with newly discovered node")

	if err := c.syncWithNode(node); err != nil {
		zlog.Warn().
			Str("node", node).
			Err(err).
			Msg("Failed to sync with newly discovered node")
	} else {
		zlog.Info().
			Str("node", node).
			Msg("Successfully synced with newly discovered node")
	}
}

// Join handles cluster join requests from new nodes
func (c *Coordinator) Join(ctx context.Context, req *clusterpb.JoinRequest) (*clusterpb.JoinResponse, error) {
	zlog.Info().
		Str("node_id", req.NodeId).
		Str("cluster_address", req.Address).
		Str("listen_address", req.ListenAddress).
		Msg("Received join request")

	// Validate addresses
	if req.NodeId == "" || req.Address == "" || req.ListenAddress == "" {
		return nil, fmt.Errorf("invalid join request: missing required fields")
	}

	// Add node to ring with both addresses
	isNewNode, err := c.ring.AddNode(req.NodeId, req.Address, req.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to add node to ring: %w", err)
	}

	// Reset last heartbeat and failure count
	c.mu.Lock()
	c.lastHeartbeat[req.NodeId] = time.Now()
	c.failureCount[req.NodeId] = 0
	c.mu.Unlock()

	// Warm up connection to the new node if it's a genuinely new addition
	if isNewNode {
		c.router.WarmUpConnections()
	}

	// Check if we should broadcast this join
	// We broadcast only if:
	// 1. This is genuinely a new node (not already in ring)
	// 2. We haven't recently broadcast this same join (deduplication)
	shouldBroadcast := isNewNode && !c.shouldSkipBroadcast(req.NodeId, req.Address, req.ListenAddress)

	if shouldBroadcast {
		// Broadcast the join to all other nodes in the cluster
		// This ensures all nodes learn about the new member
		// Note: We record the broadcast AFTER starting it to allow retries on failure
		go c.broadcastJoinWithCacheUpdate(req.NodeId, req.Address, req.ListenAddress)

		zlog.Debug().
			Str("node_id", req.NodeId).
			Msg("Broadcasting join to cluster")
	} else if !isNewNode {
		zlog.Debug().
			Str("node_id", req.NodeId).
			Msg("Node already in ring, skipping broadcast")
	} else {
		zlog.Debug().
			Str("node_id", req.NodeId).
			Msg("Recent duplicate broadcast detected, skipping re-broadcast")
	}

	return &clusterpb.JoinResponse{
		Success: true,
		Epoch:   c.ring.GetEpoch(),
	}, nil
}

// Leave handles graceful node departure from cluster
func (c *Coordinator) Leave(ctx context.Context, req *clusterpb.LeaveRequest) (*clusterpb.LeaveResponse, error) {
	zlog.Info().
		Str("node_id", req.NodeId).
		Msg("Received leave request")

	// Remove node from ring
	if err := c.ring.RemoveNode(req.NodeId); err != nil {
		return nil, err
	}

	// Delete last heartbeat and failure count tracking
	c.mu.Lock()
	delete(c.lastHeartbeat, req.NodeId)
	delete(c.failureCount, req.NodeId)
	c.mu.Unlock()

	// Clean up router connection to the removed node
	c.router.RemoveClient(req.NodeId)

	return &clusterpb.LeaveResponse{
		Success: true,
	}, nil
}

// Heartbeat receives heartbeat from remote node, updates tracking
func (c *Coordinator) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	zlog.Debug().
		Str("node_id", req.NodeId).
		Uint64("epoch", req.Epoch).
		Msg("Received heartbeat from node")

	// Record heartbeat success
	c.recordSuccess(req.NodeId)

	// Check if heartbeat epoch is newer and trigger sync
	localEpoch := c.ring.GetEpoch()
	if req.Epoch > localEpoch {
		zlog.Info().
			Str("from_node", req.NodeId).
			Uint64("local_epoch", localEpoch).
			Uint64("remote_epoch", req.Epoch).
			Msg("Received heartbeat with newer membership epoch, triggering sync")

		// Get the node's address from our ring to sync with it
		// Do this atomically to avoid race conditions
		nodeAddr := c.getNodeAddress(req.NodeId)

		if nodeAddr != "" {
			// Trigger async sync to avoid blocking heartbeat response
			// Use a copy of the address to avoid any potential race
			syncAddr := nodeAddr
			go func() {
				// Add timeout to prevent hanging
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if err := c.syncClusterStateWithContext(ctx, syncAddr); err != nil {
					zlog.Warn().
						Err(err).
						Str("from_node", req.NodeId).
						Str("sync_addr", syncAddr).
						Msg("Failed to sync cluster state after detecting newer epoch")

					// Note: We do NOT increment failure count here because:
					// 1. The heartbeat itself was successful (node is alive)
					// 2. Sync failure could be due to transient network issues
					// 3. We don't want to mark healthy nodes as down due to sync issues
				} else {
					zlog.Debug().
						Str("from_node", req.NodeId).
						Str("sync_addr", syncAddr).
						Msg("Successfully synced cluster state after epoch mismatch")
				}
			}()
		} else {
			zlog.Warn().
				Str("node_id", req.NodeId).
				Msg("Received heartbeat from unknown node with newer epoch")
		}
	}

	return &clusterpb.HeartbeatResponse{
		Epoch: c.ring.GetEpoch(),
	}, nil
}

// GetClusterState returns current cluster membership for new nodes
func (c *Coordinator) GetClusterState(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterState, error) {
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Received request for cluster state")

	// Get all nodes from ring
	nodes := c.ring.GetAllNodes()
	pbNodes := make([]*clusterpb.NodeInfo, 0, len(nodes))

	// Convert nodes to protobuf format
	for _, node := range nodes {
		pbNode := &clusterpb.NodeInfo{
			Id:            node.ID,
			Address:       node.Address,
			ListenAddress: node.ListenAddress,
			Status:        clusterpb.NodeStatus(node.Status),
			JoinedAt:      uint64(node.JoinedAt.Unix()),
		}
		pbNodes = append(pbNodes, pbNode)
	}

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Int("node_count", len(pbNodes)).
		Msg("Returning cluster state")

	return &clusterpb.ClusterState{
		Epoch: c.ring.GetEpoch(),
		Nodes: pbNodes,
	}, nil
}

// GetClusterTopology returns full cluster topology including partition ownership
func (c *Coordinator) GetClusterTopology(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterTopology, error) {
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Received request for cluster topology")

	// Get all nodes from ring
	nodes := c.ring.GetAllNodes()
	pbNodes := make([]*clusterpb.NodeInfo, 0, len(nodes))

	// Convert nodes to protobuf format
	for _, node := range nodes {
		pbNode := &clusterpb.NodeInfo{
			Id:            node.ID,
			Address:       node.Address,
			ListenAddress: node.ListenAddress,
			Status:        clusterpb.NodeStatus(node.Status),
			JoinedAt:      uint64(node.JoinedAt.Unix()),
		}
		pbNodes = append(pbNodes, pbNode)
	}

	// Get ring configuration
	partitionCount, replicationFactor, load := c.ring.GetRingConfig()
	ringConfig := &clusterpb.RingConfig{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              load,
	}

	// Get partition ownership mapping
	partitionOwners := c.ring.GetAllPartitionOwners()
	pbPartitionOwners := make([]*clusterpb.PartitionOwner, 0, len(partitionOwners))
	for partitionID, nodeID := range partitionOwners {
		pbPartitionOwners = append(pbPartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: partitionID,
			NodeId:      nodeID,
		})
	}

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Int("node_count", len(pbNodes)).
		Int("partition_count", len(pbPartitionOwners)).
		Msg("Returning cluster topology")

	return &clusterpb.ClusterTopology{
		Epoch:           c.ring.GetEpoch(),
		Nodes:           pbNodes,
		RingConfig:      ringConfig,
		PartitionOwners: pbPartitionOwners,
	}, nil
}

// GetRing returns the consistent hash ring
func (c *Coordinator) GetRing() *Ring {
	return c.ring
}

// GetRouter returns the router
func (c *Coordinator) GetRouter() *Router {
	return c.router
}

// ErrorChan returns a channel for receiving fatal coordinator errors
func (c *Coordinator) ErrorChan() <-chan error {
	return c.errCh
}

// IsLocal checks if the key belongs to the local node
func (c *Coordinator) IsLocal(key string) bool {
	return c.router.IsLocal(key)
}

// Route returns a client for routing requests for the given key
func (c *Coordinator) Route(key string) (pb.CacheServiceClient, error) {
	return c.router.Route(key)
}

// GetNodeForKey returns the node for the given key
func (c *Coordinator) GetNodeForKey(key string) (*NodeInfo, error) {
	return c.ring.GetNode(key)
}

// GetLocalNodeID returns the ID of the local node
func (c *Coordinator) GetLocalNodeID() string {
	return c.config.MyNodeID
}

// getNodeAddress safely retrieves a node's address from the ring
func (c *Coordinator) getNodeAddress(nodeID string) string {
	nodes := c.ring.GetAllNodes()
	for _, node := range nodes {
		if node.ID == nodeID {
			return node.Address
		}
	}
	return ""
}

// syncClusterStateWithContext synchronizes the local cluster state with a remote node using provided context
func (c *Coordinator) syncClusterStateWithContext(ctx context.Context, nodeAddr string) error {
	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("sync_with", nodeAddr).
		Msg("Syncing cluster state with remote node")

	conn, err := grpc.DialContext(ctx, nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to node %s: %w", nodeAddr, err)
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	// Get current cluster state from the remote node
	state, err := client.GetClusterState(ctx, &clusterpb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state from node %s: %w", nodeAddr, err)
	}

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Str("sync_with", nodeAddr).
		Int("node_count", len(state.Nodes)).
		Uint64("remote_epoch", state.Epoch).
		Uint64("local_epoch", c.ring.GetEpoch()).
		Msg("Received cluster state from remote node")

	// Only sync if remote epoch is newer
	if state.Epoch <= c.ring.GetEpoch() {
		zlog.Debug().
			Str("node_id", c.config.MyNodeID).
			Uint64("remote_epoch", state.Epoch).
			Uint64("local_epoch", c.ring.GetEpoch()).
			Msg("Remote epoch not newer, skipping sync")
		return nil
	}

	// Apply the state to our local ring
	successCount := 0
	for _, node := range state.Nodes {
		// Add node to our ring (idempotent operation)
		if _, err := c.ring.AddNode(node.Id, node.Address, node.ListenAddress); err != nil {
			zlog.Warn().
				Err(err).
				Str("node_id", node.Id).
				Msg("Failed to add node during sync")
		} else {
			successCount++
		}
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("sync_with", nodeAddr).
		Int("nodes_synced", successCount).
		Int("total_nodes", len(state.Nodes)).
		Uint64("new_epoch", c.ring.GetEpoch()).
		Msg("Completed cluster state sync")

	return nil
}

// shouldSkipBroadcast checks if a broadcast should be skipped due to recent duplicate
func (c *Coordinator) shouldSkipBroadcast(nodeID, address, listenAddress string) bool {
	broadcastKey := fmt.Sprintf("%s:%s:%s", nodeID, address, listenAddress)

	c.broadcastCacheMu.RLock()
	lastTime, exists := c.broadcastCache[broadcastKey]
	c.broadcastCacheMu.RUnlock()

	if !exists {
		return false
	}

	// Skip if we've seen this broadcast in the last DefaultBroadcastCacheTime
	return time.Since(lastTime) < DefaultBroadcastCacheTime
}

// recordBroadcast records that a broadcast was sent
func (c *Coordinator) recordBroadcast(nodeID, address, listenAddress string) {
	c.broadcastCacheMu.Lock()
	defer c.broadcastCacheMu.Unlock()

	broadcastKey := fmt.Sprintf("%s:%s:%s", nodeID, address, listenAddress)
	c.broadcastCache[broadcastKey] = time.Now()

	// Clean old entries to prevent memory leak
	if len(c.broadcastCache) > 1000 {
		c.cleanBroadcastCache()
	}
}

// cleanBroadcastCache removes old entries from the broadcast cache
func (c *Coordinator) cleanBroadcastCache() {
	// Already holding the lock
	cutoff := time.Now().Add(-DefaultBroadcastCacheTime)
	for key, timestamp := range c.broadcastCache {
		if timestamp.Before(cutoff) {
			delete(c.broadcastCache, key)
		}
	}
}

// broadcastJoinWithCacheUpdate broadcasts join and updates cache only after successful broadcasts
func (c *Coordinator) broadcastJoinWithCacheUpdate(newNodeID, newNodeAddr, newNodeListenAddr string) {
	// Mark as sent only after we've successfully sent at least one broadcast
	var successfulBroadcasts int32
	var wg sync.WaitGroup

	c.broadcastJoin(newNodeID, newNodeAddr, newNodeListenAddr, &successfulBroadcasts, &wg)

	// Wait for all broadcasts to complete
	wg.Wait()

	// Now check if any were successful
	if atomic.LoadInt32(&successfulBroadcasts) > 0 {
		c.recordBroadcast(newNodeID, newNodeAddr, newNodeListenAddr)
		zlog.Debug().
			Str("node_id", newNodeID).
			Int32("successful_broadcasts", atomic.LoadInt32(&successfulBroadcasts)).
			Msg("Recorded successful broadcast in cache")
	}
}

// broadcastJoin notifies all active nodes about a new member joining
func (c *Coordinator) broadcastJoin(newNodeID, newNodeAddr, newNodeListenAddr string, successCounter *int32, wg *sync.WaitGroup) {
	nodes := c.ring.GetActiveNodes()

	// Limit broadcasts to prevent storms
	maxBroadcasts := 10
	actualBroadcasts := 0

	// Count eligible nodes (excluding self and new node)
	eligibleNodes := 0
	for _, node := range nodes {
		if node.ID != c.config.MyNodeID && node.ID != newNodeID {
			eligibleNodes++
		}
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("new_node", newNodeID).
		Int("eligible_nodes", eligibleNodes).
		Int("max_broadcasts", maxBroadcasts).
		Msg("Broadcasting join event to cluster")

	for _, node := range nodes {
		// Skip self and the new node
		if node.ID == c.config.MyNodeID || node.ID == newNodeID {
			continue
		}

		// Limit number of broadcasts
		if actualBroadcasts >= maxBroadcasts {
			remainingNodes := eligibleNodes - actualBroadcasts
			zlog.Warn().
				Int("sent", actualBroadcasts).
				Int("skipped", remainingNodes).
				Msg("Reached broadcast limit, skipping remaining nodes")
			break
		}
		actualBroadcasts++

		// Add to WaitGroup before launching goroutine
		if wg != nil {
			wg.Add(1)
		}

		// Broadcast asynchronously to avoid blocking
		go func(targetNode *NodeInfo) {
			if wg != nil {
				defer wg.Done()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := grpc.DialContext(ctx, targetNode.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err != nil {
				zlog.Warn().
					Err(err).
					Str("target_node", targetNode.ID).
					Str("new_node", newNodeID).
					Msg("Failed to connect for join broadcast")
				return
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)

			// Send join request to propagate the new member
			joinReq := &clusterpb.JoinRequest{
				NodeId:        newNodeID,
				Address:       newNodeAddr,
				ListenAddress: newNodeListenAddr,
			}

			_, err = client.Join(ctx, joinReq)
			if err != nil {
				zlog.Warn().
					Err(err).
					Str("target_node", targetNode.ID).
					Str("new_node", newNodeID).
					Msg("Failed to broadcast join to node")
			} else {
				if successCounter != nil {
					atomic.AddInt32(successCounter, 1)
				}
				zlog.Debug().
					Str("target_node", targetNode.ID).
					Str("new_node", newNodeID).
					Msg("Successfully broadcast join to node")
			}
		}(node)
	}
}
