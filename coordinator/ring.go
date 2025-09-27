package coordinator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/hash"
)

// NodeStatus represents the status of a node in the cluster
type NodeStatus int

const (
	NodeStatusActive NodeStatus = iota
	NodeStatusJoining
	NodeStatusLeaving
	NodeStatusDown
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusActive:
		return "active"
	case NodeStatusJoining:
		return "joining"
	case NodeStatusLeaving:
		return "leaving"
	case NodeStatusDown:
		return "down"
	default:
		return "unknown"
	}
}

// NodeInfo stores information about a node in the cluster
type NodeInfo struct {
	ID            string
	Address       string // Cluster communication address (for heartbeats, etc.)
	ListenAddress string // Service listen address for client requests (Put/Get/Delete)
	Status        NodeStatus
	JoinedAt      time.Time
	Weight        float64
	Available     bool // Tracks if node is available for routing
}

// nodeMember implements consistent.Member interface
type nodeMember string

func (n nodeMember) String() string {
	return string(n)
}

// Ring manages consistent hash ring for key-to-node mapping.
// It separates membership changes (add/remove) from availability changes (up/down)
// to later support hinted handoff without unnecessary ownership transfers.
type Ring struct {
	ch             *consistent.Consistent
	nodes          map[string]*NodeInfo
	epoch          uint64 // Only incremented on add/remove of nodes
	mu             sync.RWMutex
	partitionCount int
	localNodeID    string
}

// NewRing creates a new consistent hash ring
func NewRing(partitionCount int, localNodeID string) (*Ring, error) {
	if partitionCount <= 0 {
		partitionCount = hash.DefaultPartitionCount
	}

	cfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: hash.DefaultReplicationFactor,
		Load:              hash.DefaultLoad,
		Hasher:            hash.Hasher{},
	}

	ch := consistent.New(nil, cfg)

	return &Ring{
		ch:             ch,
		nodes:          make(map[string]*NodeInfo),
		partitionCount: partitionCount,
		localNodeID:    localNodeID,
	}, nil
}

// AddNode adds a new node with both cluster and listen addresses
// It is idempotent - if the node already exists with the same addresses, it returns success
func (r *Ring) AddNode(id, address, listenAddress string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Both addresses are required
	if address == "" {
		return fmt.Errorf("cluster address is required for node %s", id)
	}
	if listenAddress == "" {
		return fmt.Errorf("listen address is required for node %s", id)
	}

	// Check if node already exists
	if existingNode, exists := r.nodes[id]; exists {
		// If the node exists with the same addresses, consider it a success (idempotent)
		if existingNode.Address == address && existingNode.ListenAddress == listenAddress {
			zlog.Debug().
				Str("node_id", id).
				Str("cluster_address", address).
				Str("listen_address", listenAddress).
				Msg("Node already exists in ring with same addresses, treating as success")
			return nil
		}
		// If addresses differ, it's an error
		return fmt.Errorf("node %s already exists with different addresses (existing: %s/%s, new: %s/%s)",
			id, existingNode.Address, existingNode.ListenAddress, address, listenAddress)
	}

	// Add to consistent hash ring
	member := nodeMember(id)
	r.ch.Add(member)

	// Add to node registry
	r.nodes[id] = &NodeInfo{
		ID:            id,
		Address:       address,
		ListenAddress: listenAddress,
		Status:        NodeStatusActive,
		JoinedAt:      time.Now(),
		Weight:        1.0,
		Available:     true, // New nodes start as available
	}

	// Increment membership epoch (true membership change)
	atomic.AddUint64(&r.epoch, 1)

	zlog.Info().
		Str("node_id", id).
		Str("cluster_address", address).
		Str("listen_address", listenAddress).
		Uint64("membership_epoch", r.epoch).
		Msg("Added node to ring")

	return nil
}

// RemoveNode permanently removes a node from the cluster (true membership change)
func (r *Ring) RemoveNode(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not found in ring", id)
	}

	// Remove from consistent hash ring
	r.ch.Remove(id)

	// Delete from registry
	delete(r.nodes, id)

	// Increment membership epoch (true membership change)
	atomic.AddUint64(&r.epoch, 1)

	zlog.Info().
		Str("node_id", id).
		Uint64("membership_epoch", r.epoch).
		Msg("Removed node from ring")

	return nil
}

// UpdateNodeStatus updates node availability without removing from ring.
// Preserves ownership mapping for temporary node changes.
func (r *Ring) UpdateNodeStatus(id string, status NodeStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not found", id)
	}

	oldStatus := node.Status
	node.Status = status

	// Update availability based on status
	// Node remains in ring but availability changes
	switch status {
	case NodeStatusActive:
		node.Available = true
	case NodeStatusDown, NodeStatusLeaving:
		node.Available = false
	case NodeStatusJoining:
		node.Available = false // Not available until fully joined
	}

	zlog.Info().
		Str("node_id", id).
		Str("old_status", oldStatus.String()).
		Str("new_status", status.String()).
		Bool("available", node.Available).
		Uint64("membership_epoch", r.epoch).
		Msg("Updated node status")

	return nil
}

// GetNode returns the available node that owns the key.
// Returns error if owner is unavailable.
func (r *Ring) GetNode(key string) (*NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	member := r.ch.LocateKey([]byte(key))
	if member == nil {
		return nil, fmt.Errorf("no node available for key %s", key)
	}

	node, exists := r.nodes[member.String()]
	if !exists {
		return nil, fmt.Errorf("node %s not found in registry", member.String())
	}

	// Check availability (not just status)
	if !node.Available {
		// In Phase 1: return error
		// In Phase 2: this is where we'd return the temporary owner
		return nil, fmt.Errorf("node %s is not available (status: %s)", node.ID, node.Status)
	}

	return node, nil
}

// GetPrimaryNode returns the primary owner regardless of availability.
// Used in Phase 2 to identify hint recipients.
func (r *Ring) GetPrimaryNode(key string) (*NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	member := r.ch.LocateKey([]byte(key))
	if member == nil {
		return nil, fmt.Errorf("no node in ring for key %s", key)
	}

	node, exists := r.nodes[member.String()]
	if !exists {
		return nil, fmt.Errorf("node %s not found in registry", member.String())
	}

	// Return node regardless of availability
	return node, nil
}

// GetNextAvailableNode finds the next available node in the ring.
// Phase 2 uses this for temporary ownership during failures.
func (r *Ring) GetNextAvailableNode(key string) (*NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members, err := r.ch.GetClosestN([]byte(key), len(r.nodes))
	if err != nil {
		return nil, err
	}

	// Find first available node
	for _, member := range members {
		if node, exists := r.nodes[member.String()]; exists && node.Available {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no available nodes in cluster")
}

// GetAllNodes returns all nodes in the cluster
func (r *Ring) GetAllNodes() []*NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	return nodes
}

// GetActiveNodes returns all active nodes in the cluster
func (r *Ring) GetActiveNodes() []*NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		if node.Status == NodeStatusActive {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

// GetAvailableNodes returns nodes that are available for routing
func (r *Ring) GetAvailableNodes() []*NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		if node.Available {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

// GetEpoch returns the epoch for membership changes
func (r *Ring) GetEpoch() uint64 {
	return atomic.LoadUint64(&r.epoch)
}

// IsLocal checks if the local node is the owner of the key
func (r *Ring) IsLocal(key string) bool {
	node, err := r.GetPrimaryNode(key)
	if err != nil {
		return false
	}
	return node.ID == r.localNodeID
}

// IsNodeAvailable checks if a specific node is available
func (r *Ring) IsNodeAvailable(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if node, exists := r.nodes[nodeID]; exists {
		return node.Available
	}
	return false
}

// GetPartitionForKey returns the partition for the key
func (r *Ring) GetPartitionForKey(key string) int {
	return r.ch.FindPartitionID([]byte(key))
}

// GetNodeForPartition returns the node for the partition
func (r *Ring) GetNodeForPartition(partition int) (*NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	owner := r.ch.GetPartitionOwner(partition)
	if owner == nil {
		return nil, fmt.Errorf("no owner for partition %d", partition)
	}

	node, exists := r.nodes[owner.String()]
	if !exists {
		return nil, fmt.Errorf("node %s not found", owner.String())
	}

	return node, nil
}

// GetClosestN returns the closest n nodes for the key
func (r *Ring) GetClosestN(key string, count int) ([]*NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members, err := r.ch.GetClosestN([]byte(key), count)
	if err != nil {
		return nil, err
	}

	nodes := make([]*NodeInfo, 0, len(members))
	for _, member := range members {
		if node, exists := r.nodes[member.String()]; exists && node.Available {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no available nodes found")
	}

	return nodes, nil
}

// GetAllPartitionOwners returns a map of all partition IDs to their owner node IDs
func (r *Ring) GetAllPartitionOwners() map[int32]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	owners := make(map[int32]string)
	for i := 0; i < r.partitionCount; i++ {
		owner := r.ch.GetPartitionOwner(i)
		if owner != nil {
			owners[int32(i)] = owner.String()
		}
	}
	return owners
}

// GetRingConfig returns the configuration of the consistent hash ring
func (r *Ring) GetRingConfig() (partitionCount int32, replicationFactor int32, load float64) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return the actual configuration used by this ring
	return int32(r.partitionCount), hash.DefaultReplicationFactor, hash.DefaultLoad
}
