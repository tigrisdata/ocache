package cacheclient

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

// TopologyManager manages cluster topology for ClusterClient.
// It uses a token-based ring that matches the server's dskit ring implementation,
// ensuring consistent key routing between client and server.
type TopologyManager struct {
	ring *TokenRing // Token-based ring for key lookup

	seedAddrs       []string      // seed addresses
	topologyEpoch   atomic.Uint64 // Content-addressable epoch (hash of ring state)
	refreshInterval time.Duration
	dialOpts        []grpc.DialOption
}

// NewTopologyManager creates a new topology manager
func NewTopologyManager(seedAddrs []string, refreshInterval time.Duration, dialOpts []grpc.DialOption) (*TopologyManager, error) {
	tm := &TopologyManager{
		ring:            NewTokenRing(),
		seedAddrs:       seedAddrs,
		refreshInterval: refreshInterval,
		dialOpts:        dialOpts,
	}

	if err := tm.initialize(); err != nil {
		return nil, err
	}

	return tm, nil
}

// initialize initializes the topology manager with the given seed addresses
func (tm *TopologyManager) initialize() error {
	var topology *clusterpb.ClusterTopology
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()

	// Try each address
	for _, addr := range tm.seedAddrs {
		topology, err = tm.fetchTopologyFromAddress(ctx, addr)
		if err == nil {
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to fetch topology from any node: %w", err)
	}

	tm.UpdateTopology(topology)

	return nil
}

// FetchTopology fetches the cluster topology from available nodes
func (tm *TopologyManager) FetchTopology(ctx context.Context) (*clusterpb.ClusterTopology, error) {
	// If we have existing topology, try those nodes
	nodeAddresses := tm.GetNodeAddresses()
	for _, addr := range nodeAddresses {
		topology, err := tm.fetchTopologyFromAddress(ctx, addr)
		if err == nil {
			return topology, nil
		}
	}

	// If we failed to fetch topology from any known node, try the seed addresses
	for _, addr := range tm.seedAddrs {
		topology, err := tm.fetchTopologyFromAddress(ctx, addr)
		if err == nil {
			return topology, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch topology from any node or seed address")
}

// RefreshTopology refreshes the topology
func (tm *TopologyManager) RefreshTopology(ctx context.Context) (bool, error) {
	topology, err := tm.FetchTopology(ctx)
	if err != nil {
		return false, err
	}

	_, changed := tm.UpdateTopology(topology)
	return changed, nil
}

// fetchTopologyFromAddress fetches topology from a specific address
func (tm *TopologyManager) fetchTopologyFromAddress(ctx context.Context, addr string) (*clusterpb.ClusterTopology, error) {
	conn, err := grpc.DialContext(ctx, addr, tm.dialOpts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Use CacheService.GetTopology instead of ClusterService
	client := pb.NewCacheServiceClient(conn)
	resp, err := client.GetTopology(ctx, &pb.GetTopologyRequest{})
	if err != nil {
		return nil, err
	}

	// Check for error in response
	if resp.Error != "" {
		return nil, fmt.Errorf("topology error: %s", resp.Error)
	}

	// Return the topology directly since it's now the same type
	return resp.Topology, nil
}

// UpdateTopology updates the internal state based on new topology.
// With content-addressable epochs, same epoch = same ring state, so we
// use equality check (not >=) to detect changes.
func (tm *TopologyManager) UpdateTopology(topology *clusterpb.ClusterTopology) (map[string]bool, bool) {
	// Content-addressable epochs: same epoch means identical ring state.
	// Use atomic load for lock-free check.
	currentEpoch := tm.topologyEpoch.Load()
	if currentEpoch == topology.Epoch {
		return nil, false // Same ring state, no update needed
	}

	// Build active nodes and addresses
	activeNodes := make(map[string]bool)
	nodeAddresses := make(map[string]string)
	for _, node := range topology.Nodes {
		if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
			// Use listen address for client connections
			listenAddr := node.ListenAddress
			if listenAddr == "" {
				// ListenAddress is required - skip improperly configured nodes
				continue
			}
			activeNodes[node.Id] = true
			nodeAddresses[node.Id] = listenAddr
		}
	}

	// Build token map from RingConfig.NodeTokens
	nodeTokens := make(map[string][]uint32)
	if topology.RingConfig != nil {
		for _, nt := range topology.RingConfig.NodeTokens {
			nodeTokens[nt.NodeId] = nt.Tokens
		}
	}

	// Update the token ring (thread-safe internally)
	tm.ring.Update(nodeTokens, nodeAddresses)

	// Atomic store for epoch - no lock needed
	tm.topologyEpoch.Store(topology.Epoch)

	return activeNodes, true
}

// GetNodeForKey returns the node address for a given key.
// Uses FNV-1a 32-bit hash + binary search (same as server).
func (tm *TopologyManager) GetNodeForKey(key string) (string, error) {
	return tm.ring.GetNodeForKey(key)
}

// GetNodeIDForKey returns the node ID for a given key.
// Useful for debugging and testing.
func (tm *TopologyManager) GetNodeIDForKey(key string) (string, error) {
	return tm.ring.GetNodeIDForKey(key)
}

// GetTopologyEpoch returns the current topology epoch.
// Uses atomic load for lock-free access.
func (tm *TopologyManager) GetTopologyEpoch() uint64 {
	return tm.topologyEpoch.Load()
}

// GetNodeAddresses returns all node addresses
func (tm *TopologyManager) GetNodeAddresses() map[string]string {
	return tm.ring.GetNodeAddresses()
}

// GetRing returns the underlying token ring
func (tm *TopologyManager) GetRing() *TokenRing {
	return tm.ring
}

// TopologyRefreshLoop periodically refreshes the cluster topology
func (tm *TopologyManager) TopologyRefreshLoop(ctx context.Context, updateFn func()) {
	ticker := time.NewTicker(tm.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
			changed, err := tm.RefreshTopology(fetchCtx)
			cancel()

			if err != nil {
				continue
			}

			if changed && updateFn != nil {
				updateFn()
			}
		case <-ctx.Done():
			return
		}
	}
}
