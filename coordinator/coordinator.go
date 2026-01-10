package coordinator

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/coordinator/gossip"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
)

const (
	// MaxMessageSize is the maximum message size for gRPC messages
	MaxMessageSize = 128 * 1024 * 1024 // 128MB
)

// Config contains the configuration for the coordinator
type Config struct {
	Enabled     bool     // Whether the coordinator is enabled
	MyNodeID    string   // The ID of the node
	ClusterAddr string   // The address for memberlist gossip (host:port format, e.g., "0.0.0.0:7946")
	ListenAddr  string   // The address the node listens on for client requests (Put/Get/Delete and cluster topology)
	Seeds       []string // Seed nodes for joining cluster (memberlist addresses of other nodes)
	DiskPath    string   // The path to the disk for persisting ring tokens

	// LifecyclerConfig allows advanced ring configuration (optional).
	// Mainly used for testing.
	LifecyclerConfig ring.LifecyclerConfig

	// Router configuration
	RouterConfig *RouterConfig

	// Registerer is the prometheus registerer to use. If nil, uses prometheus.DefaultRegisterer.
	// This is useful for tests to avoid duplicate registration panics.
	Registerer prometheus.Registerer
}

// Coordinator manages cluster membership, request routing, and cluster RPC handling.
// Uses dskit ring + memberlist for gossip-based membership.
// Note: GetClusterState/GetClusterTopology RPCs are registered on the main server gRPC service.
type Coordinator struct {
	clusterpb.UnimplementedClusterServiceServer

	// Configuration
	config *Config

	// dskit ring components
	memberlistKV *gossip.Memberlist
	ringManager  *ring.RingManager
	router       *Router

	// Lifecycle management
	stopCh chan struct{}
	errCh  chan error // Channel for propagating fatal errors

	// Logger adapter for dskit
	logger log.Logger

	// Prometheus registry
	reg prometheus.Registerer
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
		Str("listen_addr", config.ListenAddr).
		Msg("Creating new coordinator")

	// Validate node ID format - must be safe for use in memberlist gossip protocol
	if err := validateNodeID(config.MyNodeID); err != nil {
		return nil, fmt.Errorf("invalid node ID: %w", err)
	}

	if config.ListenAddr == "" {
		return nil, fmt.Errorf("listen address is required in cluster mode")
	}

	if config.DiskPath == "" {
		return nil, fmt.Errorf("disk path is required in cluster mode")
	}

	// Create logger adapter for dskit - wraps zerolog to go-kit/log interface
	logger := &zerologAdapter{}

	// Create prometheus registry (use provided or default)
	reg := config.Registerer
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	// Create memberlist KV for ring state storage
	memberlistKV, err := gossip.NewMemberlist(config.MyNodeID, config.ClusterAddr, config.Seeds, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist KV: %w", err)
	}

	// Create lifecycler config
	if err := ring.SetupLifecyclerConfig(config.MyNodeID, config.ListenAddr, config.DiskPath, &config.LifecyclerConfig); err != nil {
		return nil, fmt.Errorf("failed to create lifecycler config: %w", err)
	}

	// Create ring manager
	ringManager, err := ring.NewRingManager(config.LifecyclerConfig, memberlistKV.Client(), logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring manager: %w", err)
	}

	// Create router with ring manager
	var router *Router
	if config.RouterConfig != nil {
		router = NewRouterWithConfig(ringManager, config.MyNodeID, config.RouterConfig)
	} else {
		router = NewRouter(ringManager, config.MyNodeID)
	}

	coord := &Coordinator{
		config:       config,
		memberlistKV: memberlistKV,
		ringManager:  ringManager,
		router:       router,
		stopCh:       make(chan struct{}),
		errCh:        make(chan error, 1), // Buffered to prevent blocking
		logger:       logger,
		reg:          reg,
	}

	return coord, nil
}

// Start starts the coordinator and joins the cluster
func (c *Coordinator) Start(ctx context.Context) error {
	// Start memberlist KV for gossip-based state sharing
	if err := c.memberlistKV.Start(ctx); err != nil {
		return fmt.Errorf("failed to start memberlist KV: %w", err)
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Strs("seeds", c.config.Seeds).
		Msg("Memberlist KV started")

	// Start ring manager (ring + lifecycler)
	if err := c.ringManager.Start(ctx); err != nil {
		// Cleanup memberlist on failure
		_ = c.memberlistKV.Stop(ctx)
		return fmt.Errorf("failed to start ring manager: %w", err)
	}

	zlog.Info().
		Str("node_id", c.config.MyNodeID).
		Str("cluster_addr", c.config.ClusterAddr).
		Str("listen_addr", c.config.ListenAddr).
		Uint64("epoch", c.ringManager.GetEpoch()).
		Msg("Coordinator started")

	return nil
}

// Stop stops the coordinator and cleans up resources
func (c *Coordinator) Stop() error {
	close(c.stopCh)

	ctx := context.Background()

	// Close router connections first
	if err := c.router.Close(); err != nil {
		zlog.Error().Err(err).Msg("Error closing router connections")
	}

	// Announce leaving BEFORE stopping ring manager.
	// This broadcasts LEAVING state while memberlist is still running,
	// ensuring other nodes are notified of our departure.
	if err := c.ringManager.AnnounceLeaving(ctx); err != nil {
		zlog.Warn().Err(err).Msg("Failed to announce leaving (continuing with shutdown)")
	}

	// Stop ring manager (will complete the unregister)
	if err := c.ringManager.Stop(ctx); err != nil {
		zlog.Error().Err(err).Msg("Error stopping ring manager")
	}

	// Stop memberlist KV
	if err := c.memberlistKV.Stop(ctx); err != nil {
		zlog.Error().Err(err).Msg("Error stopping memberlist KV")
	}

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Coordinator stopped successfully")

	return nil
}

// GetClusterState returns current cluster membership for clients
func (c *Coordinator) GetClusterState(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterState, error) {
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Received request for cluster state")

	// Get all nodes from ring
	nodes := c.ringManager.GetAllNodes()
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
		Epoch: c.ringManager.GetEpoch(),
		Nodes: pbNodes,
	}, nil
}

// GetClusterTopology returns full cluster topology including token assignments for routing
func (c *Coordinator) GetClusterTopology(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterTopology, error) {
	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Msg("Received request for cluster topology")

	// Get all nodes from ring
	nodes := c.ringManager.GetAllNodes()
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

	// Get token assignments from ring for each node
	nodeTokens := c.ringManager.GetNodeTokens()
	pbNodeTokens := make([]*clusterpb.NodeTokens, 0, len(nodeTokens))
	for nodeID, tokens := range nodeTokens {
		pbNodeTokens = append(pbNodeTokens, &clusterpb.NodeTokens{
			NodeId: nodeID,
			Tokens: tokens,
		})
	}

	// Ring config with token assignments for clients
	// ReplicationFactor here is data replication (1 = no replication)
	ringConfig := &clusterpb.RingConfig{
		ReplicationFactor: int32(c.config.LifecyclerConfig.RingConfig.ReplicationFactor),
		NodeTokens:        pbNodeTokens,
	}

	zlog.Debug().
		Str("node_id", c.config.MyNodeID).
		Int("node_count", len(pbNodes)).
		Int("token_node_count", len(pbNodeTokens)).
		Msg("Returning cluster topology")

	return &clusterpb.ClusterTopology{
		Epoch:      c.ringManager.GetEpoch(),
		Nodes:      pbNodes,
		RingConfig: ringConfig,
	}, nil
}

// GetRing returns the ring manager
func (c *Coordinator) GetRing() *ring.RingManager {
	return c.ringManager
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
	return c.ringManager.IsLocal(key)
}

// Route returns a client for routing requests for the given key
func (c *Coordinator) Route(key string) (pb.CacheServiceClient, error) {
	return c.router.Route(key)
}

// GetNodeForKey returns the node for the given key
func (c *Coordinator) GetNodeForKey(key string) (*ring.NodeInfo, error) {
	return c.ringManager.GetNode(key)
}

// GetLocalNodeID returns the ID of the local node
func (c *Coordinator) GetLocalNodeID() string {
	return c.config.MyNodeID
}

// GetEpoch returns the current ring epoch
func (c *Coordinator) GetEpoch() uint64 {
	return c.ringManager.GetEpoch()
}

// IsReady returns true if the coordinator is ready to serve requests
func (c *Coordinator) IsReady() bool {
	return c.ringManager.IsReady()
}

// WaitReady blocks until the coordinator reaches ACTIVE state or the context is cancelled.
// This is useful for callers that need to wait for the cluster to be ready before proceeding.
func (c *Coordinator) WaitReady(ctx context.Context) error {
	return c.ringManager.WaitReady(ctx)
}

// validateNodeID validates the node ID format.
// Node IDs must be non-empty and safe for use in memberlist gossip protocol.
func validateNodeID(nodeID string) error {
	const maxNodeIDLength = 256

	if nodeID == "" {
		return fmt.Errorf("node ID is required in cluster mode")
	}

	if len(nodeID) > maxNodeIDLength {
		return fmt.Errorf("node ID exceeds maximum length of %d characters", maxNodeIDLength)
	}

	// Check for disallowed characters that could cause issues in gossip protocol
	for i, r := range nodeID {
		// Disallow control characters, whitespace (except space), and problematic punctuation
		if r < 32 || r == 127 { // Control characters
			return fmt.Errorf("node ID contains control character at position %d", i)
		}
		if r == '\t' || r == '\n' || r == '\r' {
			return fmt.Errorf("node ID contains whitespace character at position %d", i)
		}
	}

	return nil
}
