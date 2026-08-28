// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"fmt"
	"sync"
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
	mu              sync.RWMutex // Protects topology updates

	// retryEpoch keeps a topology refresh alive when a pool dial failed after
	// the topology itself was published. This lets the next same-epoch refresh
	// retry the missing pool instead of treating the topology as unchanged.
	retryEpoch   atomic.Uint64
	retryPending atomic.Bool
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
	if len(tm.seedAddrs) == 0 {
		return fmt.Errorf("no seed addresses provided")
	}

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

	if topology == nil {
		return fmt.Errorf("received nil topology from all nodes")
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

	// Validate topology is not nil (valid protobuf state but invalid for our use)
	if resp.Topology == nil {
		return nil, fmt.Errorf("server returned empty topology")
	}

	return resp.Topology, nil
}

// preparedTopology contains the state needed to publish a topology update.
type preparedTopology struct {
	baseEpoch   uint64
	epoch       uint64
	activeNodes map[string]bool
	activeAddrs map[string]bool
	ring        *ringState
}

// prepareTopology builds a topology update without publishing it. The client
// stages its connection pools from this state before applying it to the token
// ring, so readers never observe a ring without its new connections.
func (tm *TopologyManager) prepareTopology(topology *clusterpb.ClusterTopology) (*preparedTopology, bool) {
	return tm.prepareTopologyWithRetry(topology, false)
}

// prepareTopologyForRetry rebuilds a topology update whose epoch is already
// published but still has a pool that failed to stage.
func (tm *TopologyManager) prepareTopologyForRetry(topology *clusterpb.ClusterTopology) (*preparedTopology, bool) {
	return tm.prepareTopologyWithRetry(topology, true)
}

func (tm *TopologyManager) prepareTopologyWithRetry(topology *clusterpb.ClusterTopology, allowSameEpoch bool) (*preparedTopology, bool) {
	if topology == nil {
		return nil, false
	}

	tm.mu.RLock()
	baseEpoch := tm.topologyEpoch.Load()
	tm.mu.RUnlock()
	if !allowSameEpoch && baseEpoch == topology.Epoch {
		return nil, false
	}

	activeNodes := make(map[string]bool)
	activeAddrs := make(map[string]bool)
	nodeAddresses := make(map[string]string)
	for _, node := range topology.Nodes {
		if node.Status != clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
			continue
		}

		// Use listen address for client connections.
		listenAddr := node.ListenAddress
		if listenAddr == "" {
			// ListenAddress is required - skip improperly configured nodes.
			continue
		}
		activeNodes[node.Id] = true
		activeAddrs[listenAddr] = true
		nodeAddresses[node.Id] = listenAddr
	}

	// Build token map from RingConfig.NodeTokens, but only for active nodes.
	// This prevents routing to nodes that are DOWN or LEAVING.
	nodeTokens := make(map[string][]uint32)
	if topology.RingConfig != nil {
		for _, nt := range topology.RingConfig.NodeTokens {
			if activeNodes[nt.NodeId] {
				nodeTokens[nt.NodeId] = append([]uint32(nil), nt.Tokens...)
			}
		}
	}

	return &preparedTopology{
		baseEpoch:   baseEpoch,
		epoch:       topology.Epoch,
		activeNodes: activeNodes,
		activeAddrs: activeAddrs,
		ring:        buildRingState(nodeTokens, nodeAddresses),
	}, true
}

// applyPreparedTopology publishes a topology if the state it was prepared
// from is still current. The ring and epoch are changed together under the
// topology lock after pool staging; the client then publishes the matching
// connection set pointer.
func (tm *TopologyManager) applyPreparedTopology(update *preparedTopology) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.topologyEpoch.Load() != update.baseEpoch {
		return false
	}

	tm.ring.state.Store(update.ring)
	tm.topologyEpoch.Store(update.epoch)
	return true
}

// UpdateTopology updates the internal state based on new topology.
// With content-addressable epochs, same epoch = same ring state, so we use
// equality check (not >=) to detect changes.
func (tm *TopologyManager) UpdateTopology(topology *clusterpb.ClusterTopology) (map[string]bool, bool) {
	update, changed := tm.prepareTopology(topology)
	if !changed || !tm.applyPreparedTopology(update) {
		return nil, false
	}
	return update.activeNodes, true
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

// GetNodeInfoForKey returns both the node ID and address for a given key.
func (tm *TopologyManager) GetNodeInfoForKey(key string) (nodeID, address string, err error) {
	return tm.ring.GetNodeInfoForKey(key)
}

// GetTopologyEpoch returns the current topology epoch.
// Uses atomic load for lock-free access.
func (tm *TopologyManager) GetTopologyEpoch() uint64 {
	return tm.topologyEpoch.Load()
}

func (tm *TopologyManager) requestTopologyRetry(epoch uint64) {
	tm.retryEpoch.Store(epoch)
	tm.retryPending.Store(true)
}

func (tm *TopologyManager) shouldRetryTopology(epoch uint64) bool {
	return tm.retryPending.Load() && tm.retryEpoch.Load() == epoch
}

func (tm *TopologyManager) clearTopologyRetry() {
	tm.retryPending.Store(false)
}

// getNodeAddressesAndEpoch returns a coherent topology snapshot for connection
// reconciliation. The read lock keeps the ring state and epoch from being
// observed across the middle of an update.
func (tm *TopologyManager) getNodeAddressesAndEpoch() (map[string]string, uint64) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.ring.GetNodeAddresses(), tm.topologyEpoch.Load()
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

// TopologyRefreshLoopWithTopology refreshes topology without publishing it
// until updateFn has staged the corresponding connection pools. A pending
// same-epoch retry is also delivered so a failed pool dial can recover.
func (tm *TopologyManager) TopologyRefreshLoopWithTopology(ctx context.Context, updateFn func(*clusterpb.ClusterTopology)) {
	ticker := time.NewTicker(tm.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fetchCtx, cancel := context.WithTimeout(ctx, TopologyDetectTimeout)
			topology, err := tm.FetchTopology(fetchCtx)
			cancel()
			if err != nil || topology == nil {
				continue
			}
			if tm.GetTopologyEpoch() == topology.Epoch && !tm.shouldRetryTopology(topology.Epoch) {
				continue
			}
			if updateFn != nil {
				updateFn(topology)
			}
		case <-ctx.Done():
			return
		}
	}
}
