package cacheclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"google.golang.org/grpc"

	"github.com/tigrisdata/ocache/common/hash"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// partitionOwner represents the owner of a partition
type partitionOwner struct {
	nodeID  string
	address string
}

// TopologyManager manages cluster topology for ClusterClient
type TopologyManager struct {
	ring            *consistent.Consistent
	partitionOwners map[int32]partitionOwner

	seedAddrs       []string                   // seed addresses
	topology        *clusterpb.ClusterTopology // Current topology
	topologyEpoch   uint64                     // Topology version
	mu              sync.RWMutex
	refreshInterval time.Duration
	dialOpts        []grpc.DialOption
}

// NewTopologyManager creates a new topology manager
func NewTopologyManager(seedAddrs []string, refreshInterval time.Duration, dialOpts []grpc.DialOption) (*TopologyManager, error) {
	tm := &TopologyManager{
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

// UpdateTopology updates the internal state based on new topology
func (tm *TopologyManager) UpdateTopology(topology *clusterpb.ClusterTopology) (map[string]bool, bool) {
	// Check if topology has changed (use atomic load for consistency)
	currentEpoch := atomic.LoadUint64(&tm.topologyEpoch)
	if currentEpoch >= topology.Epoch {
		return nil, false // No change
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
	partitionOwners := make(map[int32]partitionOwner)
	nodeAddresses := make(map[string]string)
	activeNodes := make(map[string]bool)

	for _, node := range topology.Nodes {
		if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
			// Use listen address for client connections
			listenAddr := node.ListenAddress
			if listenAddr == "" {
				// ListenAddress is required - this should not happen in properly configured clusters
				// Skip this node as it's not properly configured
				continue
			}
			activeNodes[listenAddr] = true
			nodeAddresses[node.Id] = listenAddr
			ring.Add(nodeMember(node.Id))
		}
	}

	// Build explicit partition ownership from coordinator
	for _, owner := range topology.PartitionOwners {
		partitionOwners[owner.PartitionId] = partitionOwner{
			nodeID:  owner.NodeId,
			address: nodeAddresses[owner.NodeId],
		}
	}

	// Update state
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.ring = ring
	tm.partitionOwners = partitionOwners
	tm.topology = topology

	// Atomically update epoch - this invalidates all cached routing entries
	atomic.StoreUint64(&tm.topologyEpoch, topology.Epoch)

	return activeNodes, true
}

// GetNodeForKey returns the node address for a given key
func (tm *TopologyManager) GetNodeForKey(key string) (string, error) {
	// Get ring for partition computation
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	ring := tm.ring
	if ring == nil {
		return "", fmt.Errorf("ring not initialized")
	}

	// Compute partitionID from key using consistent hash
	partitionID := int32(ring.FindPartitionID([]byte(key)))

	node, exists := tm.partitionOwners[partitionID]
	if !exists {
		return "", fmt.Errorf("no owner for partition %d", partitionID)
	}

	return node.address, nil
}

// GetTopologyEpoch returns the current topology epoch
func (tm *TopologyManager) GetTopologyEpoch() uint64 {
	return atomic.LoadUint64(&tm.topologyEpoch)
}

// GetPartitionOwner returns the node ID that owns the given partition
func (tm *TopologyManager) GetPartitionOwner(partitionID int32) *partitionOwner {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	owner, exists := tm.partitionOwners[partitionID]
	if !exists {
		return nil
	}
	return &owner
}

// GetNodeAddresses returns all node addresses
func (tm *TopologyManager) GetNodeAddresses() map[string]string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]string)
	for _, v := range tm.partitionOwners {
		result[v.nodeID] = v.address
	}
	return result
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
