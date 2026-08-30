// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
)

// instanceDescPool is a sync.Pool for reusing InstanceDesc slices in hot-path ring lookups.
// This reduces GC pressure from frequent allocations during IsLocal() and GetNode() calls.
var instanceDescPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate with capacity for typical replication factors
		return make([]ring.InstanceDesc, 0, 3)
	},
}

// zonePool is a sync.Pool for reusing zone string slices in hot-path ring lookups.
var zonePool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, 3)
	},
}

// acquireInstanceDescBuffer gets an InstanceDesc slice from the pool.
func acquireInstanceDescBuffer() []ring.InstanceDesc {
	return instanceDescPool.Get().([]ring.InstanceDesc)[:0]
}

// releaseInstanceDescBuffer returns an InstanceDesc slice to the pool.
func releaseInstanceDescBuffer(buf []ring.InstanceDesc) {
	instanceDescPool.Put(buf[:0])
}

// acquireZoneBuffer gets a zone string slice from the pool.
func acquireZoneBuffer() []string {
	return zonePool.Get().([]string)[:0]
}

// releaseZoneBuffer returns a zone string slice to the pool.
func releaseZoneBuffer(buf []string) {
	zonePool.Put(buf[:0])
}

// NodeStatus represents the status of a node in the cluster
type NodeStatus int

const (
	NodeStatusActive NodeStatus = iota
	NodeStatusJoining
	NodeStatusLeaving
	NodeStatusDown
)

const (
	// RecommendedAnnounceTimeout is the recommended minimum timeout for AnnounceLeaving.
	// CAS retries can take significant time when there's contention with background
	// heartbeat operations, especially under race detection or slow CI environments.
	RecommendedAnnounceTimeout = 10 * time.Second

	// DefaultGossipPropagationDelay is the time to wait for gossip to propagate to other nodes.
	// Memberlist gossip typically propagates within 200-500ms.
	DefaultGossipPropagationDelay = 500 * time.Millisecond
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

// NodeInfo stores information about a node in the cluster.
// This maintains API compatibility with the existing coordinator package.
type NodeInfo struct {
	ID            string
	Address       string // Cluster communication address (for gossip/heartbeats)
	ListenAddress string // Service listen address for client requests (Put/Get/Delete)
	Status        NodeStatus
	JoinedAt      time.Time
	Weight        float64
	Available     bool
}

// RingManager wraps dskit's ring and lifecycler to provide the same interface
// as the existing coordinator.Ring but with production-grade features:
// - Gossip-based membership via memberlist
// - Token persistence for stable ownership
// - Proper lifecycle state machine
// - Epoch tracking via heartbeat callbacks
type RingManager struct {
	cfg LifecyclerConfig

	// dskit components
	ring       *ring.Ring
	lifecycler *ring.BasicLifecycler
	kvClient   kv.Client

	// Local node info
	localNodeID string
	localAddr   string

	// Epoch tracking - content-addressable hash of ring state.
	// Nodes with identical ring views will have identical epochs.
	// Used by clients to detect stale topology information.
	epoch *Epoch

	// stateMu protects watcher sequencing, epochs, metrics, and membership tracking.
	stateMu           sync.Mutex
	lastEpoch         uint64
	lastDeltaSequence uint64
	lastKnownNodes    map[string]ring.InstanceState // Track previous node states for delta logging
	activeNodes       int                           // Active-node metric maintained by full snapshots and deltas

	// Pre-allocated operation for GetPrimaryNode (includes all states except LEFT)
	allStatesOp ring.Operation

	// Service lifecycle
	services    *services.Manager
	subservices []services.Service

	// ctx and cancel for lifecycle management (used by delegate goroutines)
	ctx    context.Context
	cancel context.CancelFunc

	// readyCh gates the JOINING->ACTIVE transition. It is closed by MarkReady
	// once this node can actually serve requests (storage booted, gRPC server
	// listening). Until then the node stays JOINING and peers do not route
	// keyspace to it, so a still-booting or crashlooping node is never flooded
	// (issue #164).
	readyOnce sync.Once
	readyCh   chan struct{}

	// Logger adapter for dskit
	logger log.Logger

	// Prometheus registry
	reg prometheus.Registerer
}

// NewRingManager creates a new RingManager with dskit ring integration
func NewRingManager(cfg LifecyclerConfig, kvClient kv.Client, logger log.Logger, reg prometheus.Registerer) (*RingManager, error) {
	rm := &RingManager{
		cfg:         cfg,
		kvClient:    kvClient,
		localNodeID: cfg.InstanceID,
		localAddr:   cfg.InstanceAddr,
		logger:      logger,
		reg:         reg,
		epoch:       NewEpoch(),
		readyCh:     make(chan struct{}),
		// Pre-allocate the operation for GetPrimaryNode to avoid allocation on each call
		allStatesOp: ring.NewOp([]ring.InstanceState{
			ring.ACTIVE, ring.JOINING, ring.PENDING, ring.LEAVING,
		}, nil),
	}

	// Create the ring (reader/watcher)
	ringCfg := cfg.RingConfig.ToRingConfig()
	// Setting KVStore.Store to empty string tells dskit we're providing our own
	// KV client via NewWithStoreClientAndStrategy, rather than having dskit
	// create one based on the store type (consul, etcd, memberlist, etc.)
	ringCfg.KVStore.Store = ""

	var err error
	rm.ring, err = ring.NewWithStoreClientAndStrategy(
		ringCfg,
		RingName,
		RingKey,
		kvClient,
		ring.NewIgnoreUnhealthyInstancesReplicationStrategy(),
		reg,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring: %w", err)
	}

	// Create the lifecycler (manages this instance's membership)
	lifecyclerCfg := cfg.ToBasicLifecyclerConfig()
	delegate := &ringDelegate{rm: rm}

	rm.lifecycler, err = ring.NewBasicLifecycler(
		lifecyclerCfg,
		RingName,
		RingKey,
		kvClient,
		delegate,
		logger,
		reg,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create lifecycler: %w", err)
	}

	// Set up token persistence if configured
	if cfg.TokensFilePath != "" {
		// dskit's BasicLifecycler handles token persistence through the delegate
		zlog.Info().
			Str("tokens_file", cfg.TokensFilePath).
			Msg("Token persistence enabled")
	}

	// Collect subservices for lifecycle management
	rm.subservices = []services.Service{rm.ring, rm.lifecycler}

	return rm, nil
}

// Start starts the ring manager and its subservices
func (rm *RingManager) Start(ctx context.Context) error {
	// Create a cancellable context for lifecycle management
	// This context is used by delegate goroutines and cancelled on Stop()
	rm.ctx, rm.cancel = context.WithCancel(context.Background())

	// Ensure token persistence directory exists if token persistence is enabled
	if rm.cfg.TokensFilePath != "" {
		tokensDir := filepath.Dir(rm.cfg.TokensFilePath)
		if err := os.MkdirAll(tokensDir, 0o755); err != nil {
			rm.cancel() // Clean up the context
			return fmt.Errorf("failed to create tokens directory %s: %w", tokensDir, err)
		}
		zlog.Debug().
			Str("tokens_dir", tokensDir).
			Msg("Token persistence directory ensured")
	}

	var err error
	rm.services, err = services.NewManager(rm.subservices...)
	if err != nil {
		rm.cancel() // Clean up the context
		return fmt.Errorf("failed to create services manager: %w", err)
	}

	// Start all services
	if err := services.StartManagerAndAwaitHealthy(ctx, rm.services); err != nil {
		rm.cancel() // Clean up the context
		return fmt.Errorf("failed to start ring services: %w", err)
	}

	// Initialize lastKnownNodes map for tracking membership changes
	rm.lastKnownNodes = make(map[string]ring.InstanceState)

	// Start the ring watcher to detect membership changes via KV store updates.
	// This ensures we log node join/leave events immediately when they happen
	// via gossip, rather than waiting for the next heartbeat callback.
	rm.startRingWatcher(rm.ctx)

	zlog.Info().
		Str("instance_id", rm.localNodeID).
		Str("instance_addr", rm.localAddr).
		Uint64("initial_epoch", rm.epoch.Get()).
		Msg("Ring manager started")

	return nil
}

// Stop gracefully stops the ring manager
func (rm *RingManager) Stop(ctx context.Context) error {
	// Cancel the context to stop any delegate goroutines
	if rm.cancel != nil {
		rm.cancel()
	}

	if rm.services == nil {
		return nil
	}

	// Stop all services
	if err := services.StopManagerAndAwaitStopped(ctx, rm.services); err != nil {
		return fmt.Errorf("failed to stop ring services: %w", err)
	}

	zlog.Info().
		Str("instance_id", rm.localNodeID).
		Uint64("final_epoch", rm.epoch.Get()).
		Msg("Ring manager stopped")

	return nil
}

// AnnounceLeaving transitions this node to LEAVING state and waits briefly for gossip propagation.
// This should be called BEFORE Stop() to ensure other nodes are notified of the departure.
// The caller should provide a context with an appropriate timeout (recommend at least 10 seconds
// to allow CAS retries to succeed under contention).
func (rm *RingManager) AnnounceLeaving(ctx context.Context) error {
	// Transition to LEAVING state - this broadcasts via memberlist KV
	// Use the caller's context directly to respect their timeout settings
	if err := rm.lifecycler.ChangeState(ctx, ring.LEAVING); err != nil {
		return fmt.Errorf("failed to transition to LEAVING: %w", err)
	}

	level.Info(rm.logger).Log("msg", "announced leaving, waiting for propagation")

	// Brief delay to allow gossip to propagate to other nodes
	select {
	case <-time.After(DefaultGossipPropagationDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// startRingWatcher starts a goroutine that watches for ring changes via the KV store.
// Memberlist clients expose the applied merge through WatchKeyWithChanges, so the
// coordinator observes the same delta stream as the ring reader instead of causing
// another full descriptor clone for every heartbeat. Other KV backends retain the
// ordinary full-snapshot watcher.
func (rm *RingManager) startRingWatcher(ctx context.Context) {
	if watcher, ok := rm.kvClient.(interface {
		WatchKeyWithChanges(context.Context, string, func(memberlist.WatchKeyChange) bool)
	}); ok {
		go watcher.WatchKeyWithChanges(ctx, RingKey, func(change memberlist.WatchKeyChange) bool {
			if ctx.Err() != nil {
				return false
			}
			if change.FullSnapshot {
				return rm.handleRingSnapshotWithSequence(change.Value, change.Sequence)
			}

			delta, ok := change.Value.(*ring.Desc)
			if !ok || delta == nil || change.Sequence == 0 || change.TopologyChanged || len(change.ChangedKeys) == 0 || len(change.ChangedKeys) != len(delta.Ingesters) {
				return rm.refreshRingSnapshot(ctx)
			}
			var seen map[string]struct{}
			if len(change.ChangedKeys) > 1 {
				seen = make(map[string]struct{}, len(change.ChangedKeys))
			}
			for _, id := range change.ChangedKeys {
				if _, ok := delta.Ingesters[id]; !ok {
					return rm.refreshRingSnapshot(ctx)
				}
				if seen != nil {
					if _, duplicate := seen[id]; duplicate {
						return rm.refreshRingSnapshot(ctx)
					}
					seen[id] = struct{}{}
				}
			}
			rm.stateMu.Lock()
			if (rm.lastDeltaSequence == 0 && change.Sequence != 1) || (rm.lastDeltaSequence != 0 && change.Sequence != rm.lastDeltaSequence+1) {
				rm.stateMu.Unlock()
				return rm.refreshRingSnapshot(ctx)
			}
			for _, id := range change.ChangedKeys {
				if _, existed := rm.lastKnownNodes[id]; !existed {
					rm.stateMu.Unlock()
					return rm.refreshRingSnapshot(ctx)
				}
			}
			newEpoch := rm.epoch.ApplyLivenessDelta(delta.Ingesters)
			oldEpoch := rm.lastEpoch
			for id, instance := range delta.Ingesters {
				oldState := rm.lastKnownNodes[id]
				if oldState != instance.State {
					level.Info(rm.logger).Log(
						"msg", "node state changed",
						"node_id", id,
						"old_state", oldState.String(),
						"new_state", instance.State.String(),
					)
					rm.adjustActiveNodeMetric(oldState, instance.State)
				}
				rm.lastKnownNodes[id] = instance.State
			}
			rm.lastDeltaSequence = change.Sequence
			rm.lastEpoch = newEpoch
			if newEpoch != oldEpoch {
				level.Info(rm.logger).Log("msg", "ring epoch updated", "epoch", newEpoch, "node_count", len(rm.lastKnownNodes))
			}
			rm.setClusterNodeMetricsLocked()
			rm.stateMu.Unlock()
			return true
		})
		return
	}

	go func() {
		rm.kvClient.WatchKey(ctx, RingKey, func(value interface{}) bool {
			if ctx.Err() != nil {
				return false
			}
			return rm.handleRingSnapshot(value)
		})
	}()
}

func (rm *RingManager) handleRingSnapshot(value interface{}) bool {
	return rm.handleRingSnapshotWithSequence(value, 0)
}

func (rm *RingManager) handleRingSnapshotWithSequence(value interface{}, sequence uint64) bool {
	if value == nil {
		return true
	}
	ringDesc, ok := value.(*ring.Desc)
	if !ok || ringDesc == nil {
		return true
	}
	rm.stateMu.Lock()
	defer rm.stateMu.Unlock()
	if sequence > 0 && rm.lastDeltaSequence > 0 && sequence < rm.lastDeltaSequence {
		return true
	}
	newEpoch := rm.epoch.Set(ringDesc)
	membershipChanged := len(rm.lastKnownNodes) != len(ringDesc.Ingesters)
	if !membershipChanged {
		for id, instance := range ringDesc.Ingesters {
			if state, ok := rm.lastKnownNodes[id]; !ok || state != instance.State {
				membershipChanged = true
				break
			}
		}
	}
	if newEpoch != rm.lastEpoch || membershipChanged {
		rm.logMembershipChange(ringDesc, newEpoch)
		rm.lastEpoch = newEpoch
	}
	if sequence > 0 {
		rm.lastDeltaSequence = sequence
	}
	return true
}

func (rm *RingManager) refreshRingSnapshot(ctx context.Context) bool {
	var (
		value    interface{}
		sequence uint64
		err      error
	)
	if versioned, ok := rm.kvClient.(interface {
		GetWithVersion(context.Context, string) (interface{}, uint64, error)
	}); ok {
		value, sequence, err = versioned.GetWithVersion(ctx, RingKey)
	} else {
		value, err = rm.kvClient.Get(ctx, RingKey)
	}
	if err != nil {
		level.Warn(rm.logger).Log("msg", "failed to recover ring snapshot for coordinator watcher", "err", err)
		return true
	}
	return rm.handleRingSnapshotWithSequence(value, sequence)
}

// logMembershipChange logs detailed membership changes including which nodes joined/left.
// MUST be called with stateMu held.
func (rm *RingManager) logMembershipChange(ringDesc *ring.Desc, newEpoch uint64) {
	currentNodes := make(map[string]ring.InstanceState, len(ringDesc.Ingesters))
	activeCount := 0
	for id, inst := range ringDesc.Ingesters {
		currentNodes[id] = inst.State
		if inst.State == ring.ACTIVE {
			activeCount++
		}
	}

	for id, state := range currentNodes {
		if _, existed := rm.lastKnownNodes[id]; !existed {
			level.Info(rm.logger).Log("msg", "node joined", "node_id", id, "state", state.String())
		}
	}
	for id := range rm.lastKnownNodes {
		if _, exists := currentNodes[id]; !exists {
			level.Info(rm.logger).Log("msg", "node left", "node_id", id)
		}
	}
	for id, newState := range currentNodes {
		if oldState, existed := rm.lastKnownNodes[id]; existed && oldState != newState {
			level.Info(rm.logger).Log("msg", "node state changed", "node_id", id, "old_state", oldState.String(), "new_state", newState.String())
		}
	}

	rm.lastKnownNodes = currentNodes
	rm.activeNodes = activeCount
	rm.setClusterNodeMetricsLocked()
	level.Info(rm.logger).Log("msg", "ring epoch updated", "epoch", newEpoch, "node_count", len(currentNodes))
}

func (rm *RingManager) adjustActiveNodeMetric(oldState, newState ring.InstanceState) {
	if oldState == ring.ACTIVE {
		rm.activeNodes--
	}
	if newState == ring.ACTIVE {
		rm.activeNodes++
	}
}

func (rm *RingManager) setClusterNodeMetricsLocked() {
	metrics.ClusterNodes.WithLabelValues("active").Set(float64(rm.activeNodes))
	metrics.ClusterNodes.WithLabelValues("total").Set(float64(len(rm.lastKnownNodes)))
}

// tokenForKey computes a 32-bit token for the given key using FNV-1a.
// This is the hot-path function for all key lookups.
//
// We use FNV-1a (32-bit) for compatibility with dskit's hash ring.
func (rm *RingManager) tokenForKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // Write never returns an error for fnv
	return h.Sum32()
}

// IsLocal checks if the local node is the owner of the key.
func (rm *RingManager) IsLocal(key string) bool {
	token := rm.tokenForKey(key)

	// Acquire pooled buffers to reduce allocations
	instBuf := acquireInstanceDescBuffer()
	zoneBuf := acquireZoneBuffer()
	defer releaseInstanceDescBuffer(instBuf)
	defer releaseZoneBuffer(zoneBuf)

	// Get the owner from the ring using pooled buffers
	replicationSet, err := rm.ring.Get(token, ring.Write, instBuf, zoneBuf, nil)
	if err != nil {
		return false
	}

	if len(replicationSet.Instances) == 0 {
		return false
	}

	// Check if we're the owner
	isLocal := replicationSet.Instances[0].Id == rm.localNodeID
	if isLocal {
		metrics.ClusterLocalKeyChecks.WithLabelValues("local").Inc()
	} else {
		metrics.ClusterLocalKeyChecks.WithLabelValues("remote").Inc()
	}

	return isLocal
}

// GetNode returns the available node that owns the key.
func (rm *RingManager) GetNode(key string) (*NodeInfo, error) {
	metrics.ClusterKeyLookups.Inc()

	token := rm.tokenForKey(key)

	// Acquire pooled buffers to reduce allocations
	instBuf := acquireInstanceDescBuffer()
	zoneBuf := acquireZoneBuffer()
	defer releaseInstanceDescBuffer(instBuf)
	defer releaseZoneBuffer(zoneBuf)

	replicationSet, err := rm.ring.Get(token, ring.Write, instBuf, zoneBuf, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for key: %w", err)
	}

	if len(replicationSet.Instances) == 0 {
		return nil, fmt.Errorf("no node available for key %s", key)
	}

	inst := replicationSet.Instances[0]

	// Validate that the instance has a valid address
	if inst.Addr == "" {
		return nil, fmt.Errorf("node %s has no address configured", inst.Id)
	}

	return rm.instanceToNodeInfo(&inst), nil
}

// GetPrimaryNode returns the primary owner regardless of availability.
// This includes nodes in JOINING, PENDING, and LEAVING states, but NOT LEFT
// (nodes that have already departed the cluster).
func (rm *RingManager) GetPrimaryNode(key string) (*NodeInfo, error) {
	token := rm.tokenForKey(key)

	// Acquire pooled buffers to reduce allocations
	instBuf := acquireInstanceDescBuffer()
	zoneBuf := acquireZoneBuffer()
	defer releaseInstanceDescBuffer(instBuf)
	defer releaseZoneBuffer(zoneBuf)

	// Use pre-allocated operation that includes all states except LEFT
	replicationSet, err := rm.ring.Get(token, rm.allStatesOp, instBuf, zoneBuf, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary node: %w", err)
	}

	if len(replicationSet.Instances) == 0 {
		return nil, fmt.Errorf("no node in ring for key %s", key)
	}

	inst := replicationSet.Instances[0]

	// Validate that the instance has a valid address
	if inst.Addr == "" {
		return nil, fmt.Errorf("node %s has no address configured", inst.Id)
	}

	return rm.instanceToNodeInfo(&inst), nil
}

// GetAllNodes returns all nodes in the cluster.
// Returns an empty slice (not nil) if no nodes are available to ensure consistent behavior.
func (rm *RingManager) GetAllNodes() []*NodeInfo {
	replicationSet, err := rm.ring.GetAllHealthy(ring.Reporting)
	if err != nil {
		// ErrEmptyRing is expected when ring hasn't received any updates yet
		if err == ring.ErrEmptyRing {
			zlog.Debug().Str("local_node", rm.localNodeID).Msg("GetAllNodes: ring is empty (no instances yet)")
		} else {
			zlog.Debug().Err(err).Str("local_node", rm.localNodeID).Msg("GetAllNodes: GetAllHealthy failed")
		}
		// Return empty slice instead of nil for consistent behavior
		return []*NodeInfo{}
	}
	nodes := make([]*NodeInfo, 0, len(replicationSet.Instances))
	for _, inst := range replicationSet.Instances {
		nodes = append(nodes, rm.instanceToNodeInfo(&inst))
	}
	zlog.Debug().
		Int("node_count", len(nodes)).
		Str("local_node", rm.localNodeID).
		Str("local_state", string(rm.GetState())).
		Msg("GetAllNodes: returning nodes")
	return nodes
}

// GetActiveNodes returns all active nodes in the cluster.
// Returns an empty slice (not nil) if no active nodes are available.
func (rm *RingManager) GetActiveNodes() []*NodeInfo {
	replicationSet, err := rm.ring.GetReplicationSetForOperation(ring.Write)
	if err != nil {
		// Return empty slice instead of nil for consistent behavior
		return []*NodeInfo{}
	}

	nodes := make([]*NodeInfo, 0, len(replicationSet.Instances))
	for _, inst := range replicationSet.Instances {
		nodes = append(nodes, rm.instanceToNodeInfo(&inst))
	}
	return nodes
}

// GetAvailableNodes returns nodes that are available for routing
func (rm *RingManager) GetAvailableNodes() []*NodeInfo {
	return rm.GetActiveNodes()
}

// GetEpoch returns the current ring epoch.
// The epoch is a monotonically increasing counter that increments whenever
// ring membership changes (nodes join, leave, or change state).
// Clients can use this to detect stale topology information.
func (rm *RingManager) GetEpoch() uint64 {
	return rm.epoch.Get()
}

// GetNodeStatus returns the status of a specific node
func (rm *RingManager) GetNodeStatus(id string) (NodeStatus, error) {
	inst, err := rm.ring.GetInstanceState(id)
	if err != nil {
		return NodeStatusDown, fmt.Errorf("node %s not found: %w", id, err)
	}

	return rm.instanceStateToNodeStatus(inst), nil
}

// IsNodeAvailable checks if a specific node is available
func (rm *RingManager) IsNodeAvailable(nodeID string) bool {
	status, err := rm.GetNodeStatus(nodeID)
	if err != nil {
		return false
	}
	return status == NodeStatusActive
}

// GetState returns the current lifecycler state
func (rm *RingManager) GetState() ring.InstanceState {
	return rm.lifecycler.GetState()
}

// MarkReady signals that this node can serve requests, releasing the gate that
// holds it in JOINING and allowing it to advertise ACTIVE. It is called once
// storage has booted and the gRPC server is listening. Idempotent, and safe to
// call before or after the lifecycler has assigned tokens (it just closes the
// gate; the activation goroutine proceeds whenever it observes it closed).
func (rm *RingManager) MarkReady() {
	rm.readyOnce.Do(func() { close(rm.readyCh) })
}

// IsReady returns true if this instance is ready to serve requests
func (rm *RingManager) IsReady() bool {
	return rm.lifecycler.GetState() == ring.ACTIVE
}

// WaitReady blocks until the instance reaches ACTIVE state or the context is cancelled.
// This is useful for callers that need to wait for the ring to be ready before proceeding.
// Returns nil if ACTIVE state is reached, or context error if cancelled/timed out.
func (rm *RingManager) WaitReady(ctx context.Context) error {
	// Check if already ready
	if rm.IsReady() {
		return nil
	}

	// Poll until ready or context cancelled
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if rm.IsReady() {
				return nil
			}
		}
	}
}

// HealthyInstancesCount returns the count of healthy instances
func (rm *RingManager) HealthyInstancesCount() int {
	return rm.ring.InstancesCount()
}

// instanceToNodeInfo converts a dskit InstanceDesc to our NodeInfo
func (rm *RingManager) instanceToNodeInfo(inst *ring.InstanceDesc) *NodeInfo {
	return &NodeInfo{
		ID:            inst.Id,
		Address:       inst.Addr, // dskit stores the client-facing address
		ListenAddress: inst.Addr, // Same as address in dskit
		Status:        rm.instanceStateToNodeStatus(inst.State),
		JoinedAt:      time.Unix(inst.RegisteredTimestamp, 0),
		Weight:        1.0,
		Available:     inst.State == ring.ACTIVE,
	}
}

// instanceStateToNodeStatus converts dskit state to our NodeStatus
func (rm *RingManager) instanceStateToNodeStatus(state ring.InstanceState) NodeStatus {
	switch state {
	case ring.ACTIVE:
		return NodeStatusActive
	case ring.JOINING, ring.PENDING:
		return NodeStatusJoining
	case ring.LEAVING:
		return NodeStatusLeaving
	case ring.LEFT:
		return NodeStatusDown
	default:
		return NodeStatusDown
	}
}

// ringDelegate implements ring.BasicLifecyclerDelegate
type ringDelegate struct {
	rm *RingManager
}

// OnRingInstanceRegister is called when this instance registers with the ring
func (d *ringDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	// Compute epoch from ring state - this will capture the new membership
	newEpoch := d.rm.epoch.Set(&ringDesc)
	level.Info(d.rm.logger).Log("msg", "instance registering", "id", instanceID, "exists", instanceExists, "epoch", newEpoch)

	// If we have persisted tokens, load them
	if d.rm.cfg.TokensFilePath != "" {
		tokens, err := ring.LoadTokensFromFile(d.rm.cfg.TokensFilePath)
		if err == nil && len(tokens) > 0 {
			level.Info(d.rm.logger).Log("msg", "loaded persisted tokens", "count", len(tokens))
			return ring.JOINING, tokens
		}
		if err != nil {
			// Token file exists but failed to load - warn since this may cause ownership churn
			level.Warn(d.rm.logger).Log("msg", "failed to load persisted tokens, will generate new ones", "path", d.rm.cfg.TokensFilePath, "err", err)
		} else {
			level.Info(d.rm.logger).Log("msg", "no persisted tokens found, will generate new ones")
		}
	}

	// If instance exists and has tokens, reuse them
	if instanceExists && len(instanceDesc.Tokens) > 0 {
		level.Info(d.rm.logger).Log("msg", "reusing existing tokens", "count", len(instanceDesc.Tokens))
		return ring.JOINING, instanceDesc.Tokens
	}

	// Generate new tokens - BasicLifecycler doesn't generate tokens automatically,
	// so the delegate must provide them
	tokenGenerator := ring.NewRandomTokenGenerator()
	allTokens := ringDesc.GetTokens()
	newTokens := tokenGenerator.GenerateTokens(d.rm.cfg.NumTokens, allTokens)
	level.Info(d.rm.logger).Log("msg", "generated new tokens", "count", len(newTokens))
	return ring.JOINING, newTokens
}

// OnRingInstanceTokens is called when tokens are assigned to this instance.
// This is the signal that tokens are stable and we can transition to ACTIVE state.
func (d *ringDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	level.Info(d.rm.logger).Log("msg", "tokens assigned", "count", len(tokens))

	// Persist tokens if configured
	if d.rm.cfg.TokensFilePath != "" {
		if err := tokens.StoreToFile(d.rm.cfg.TokensFilePath); err != nil {
			level.Error(d.rm.logger).Log("msg", "failed to persist tokens", "err", err)
		} else {
			level.Info(d.rm.logger).Log("msg", "tokens persisted", "path", d.rm.cfg.TokensFilePath)
		}
	}

	// Update metrics
	metrics.ClusterTokensOwned.Set(float64(len(tokens)))

	// Transition to ACTIVE state now that tokens are stable.
	// The BasicLifecycler doesn't automatically transition - the delegate must call ChangeState.
	// IMPORTANT: This must be done in a goroutine because OnRingInstanceTokens is called during
	// the lifecycler's starting() phase, and ChangeState() uses an actor channel that's only
	// processed during the running() phase. Calling it synchronously would deadlock.
	go func() {
		// Gate the ACTIVE transition on readiness: stay JOINING (not routable)
		// until MarkReady signals that storage has booted and the gRPC server is
		// listening, so peers don't route keyspace to a still-booting node and a
		// crashlooping node never advertises ACTIVE (issue #164).
		select {
		case <-d.rm.readyCh:
		case <-d.rm.ctx.Done():
			return
		}

		// Use the RingManager's context so this goroutine can be cancelled on shutdown
		if err := lifecycler.ChangeState(d.rm.ctx, ring.ACTIVE); err != nil {
			// Only log error if context wasn't cancelled (normal shutdown)
			if d.rm.ctx.Err() == nil {
				level.Error(d.rm.logger).Log("msg", "failed to transition to ACTIVE state", "err", err)
			}
		} else {
			level.Info(d.rm.logger).Log("msg", "transitioned to ACTIVE state")
		}
	}()
}

// OnRingInstanceStopping is called when this instance is stopping
func (d *ringDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	// Log stopping - epoch will be updated via heartbeat callbacks as ring state changes
	level.Info(d.rm.logger).Log("msg", "instance stopping")
}

// OnRingInstanceHeartbeat is called on each heartbeat. Membership metrics and
// epoch changes are maintained by startRingWatcher, which receives the same
// memberlist delta stream as the ring reader. Keep this callback constant-time:
// the lifecycler already has the merged descriptor, but scanning it here would
// put the discarded full-ring work back on the ordinary heartbeat path.
func (d *ringDelegate) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.InstanceDesc) {
}

// GetNodeTokens returns token assignments for all active nodes in the ring.
// Used by GetClusterTopology to provide clients with token data for routing.
// Returns a map of nodeID -> sorted list of tokens. Token slices alias the
// current ring snapshot and must be treated as read-only.
//
// Important: This only returns tokens for ACTIVE nodes because:
// 1. JOINING/PENDING nodes are not yet ready to serve requests
// 2. LEAVING nodes are transitioning out and should not receive new requests
// 3. Temporarily unhealthy nodes (missed heartbeats) are filtered by GetAllHealthy
func (rm *RingManager) GetNodeTokens() map[string][]uint32 {
	result := make(map[string][]uint32)

	// Get all healthy instances from the ring.
	// Note: GetAllHealthy filters out instances that have missed heartbeats.
	replicationSet, err := rm.ring.GetAllHealthy(ring.Reporting)
	if err != nil {
		// Ring may be empty during bootstrap
		if err == ring.ErrEmptyRing {
			zlog.Debug().Msg("GetNodeTokens: ring is empty")
		} else {
			zlog.Warn().Err(err).Msg("GetNodeTokens: failed to get healthy instances")
		}
		return result
	}

	// Extract tokens from each instance
	for _, inst := range replicationSet.Instances {
		// Only include active nodes - other states (JOINING, LEAVING, etc.)
		// should not receive client traffic
		if inst.State != ring.ACTIVE {
			continue
		}

		// The ring reader publishes replacement descriptors on updates. The
		// response path treats the current snapshot's token slice as read-only.
		result[inst.Id] = inst.Tokens
	}

	zlog.Debug().
		Int("node_count", len(result)).
		Msg("GetNodeTokens: returning token assignments")

	return result
}
