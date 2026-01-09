package ring

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
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

	// stateMu protects lastEpoch used for logging epoch changes.
	stateMu   sync.Mutex
	lastEpoch uint64

	// Pre-allocated operation for GetPrimaryNode (includes all states except LEFT)
	allStatesOp ring.Operation

	// Service lifecycle
	services    *services.Manager
	subservices []services.Service

	// ctx and cancel for lifecycle management (used by delegate goroutines)
	ctx    context.Context
	cancel context.CancelFunc

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

// checkRingChangesFromDesc computes epoch from ring state using content-addressable hashing.
// Nodes with identical ring views will compute identical epochs.
// Called from heartbeat callback.
func (rm *RingManager) checkRingChangesFromDesc(ringDesc *ring.Desc) {
	if ringDesc == nil {
		return
	}

	// Compute new epoch from ring state (content-addressable hash)
	newEpoch := rm.epoch.Set(ringDesc)

	// Log if epoch changed (for debugging/monitoring)
	rm.stateMu.Lock()
	if newEpoch != rm.lastEpoch {
		level.Info(rm.logger).Log(
			"msg", "ring epoch updated",
			"epoch", newEpoch,
			"node_count", len(ringDesc.Ingesters),
		)
		rm.lastEpoch = newEpoch
	}
	rm.stateMu.Unlock()
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

// OnRingInstanceHeartbeat is called on each heartbeat
func (d *ringDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	if ringDesc == nil {
		return
	}

	// Check for ring membership changes and update epoch if needed
	d.rm.checkRingChangesFromDesc(ringDesc)

	// Update metrics
	activeCount := 0
	for _, inst := range ringDesc.Ingesters {
		if inst.State == ring.ACTIVE {
			activeCount++
		}
	}
	metrics.ClusterNodes.WithLabelValues("active").Set(float64(activeCount))
	metrics.ClusterNodes.WithLabelValues("total").Set(float64(len(ringDesc.Ingesters)))
}

// GetNodeTokens returns token assignments for all active nodes in the ring.
// Used by GetClusterTopology to provide clients with token data for routing.
// Returns a map of nodeID -> sorted list of tokens.
//
// Important: This only returns tokens for ACTIVE nodes because:
// 1. JOINING/PENDING nodes are not yet ready to serve requests
// 2. LEAVING nodes are transitioning out and should not receive new requests
// 3. Temporarily unhealthy nodes (missed heartbeats) are filtered by GetAllHealthy
func (rm *RingManager) GetNodeTokens() map[string][]uint32 {
	result := make(map[string][]uint32)

	// Get all healthy instances from the ring.
	// Note: GetAllHealthy filters out instances that have missed heartbeats.
	// This is correct - unhealthy instances should not receive client traffic.
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

		// Copy tokens to a new slice
		tokens := make([]uint32, len(inst.Tokens))
		copy(tokens, inst.Tokens)

		// Ensure tokens are sorted (they should already be, but be safe)
		sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })

		result[inst.Id] = tokens
	}

	zlog.Debug().
		Int("node_count", len(result)).
		Msg("GetNodeTokens: returning token assignments")

	return result
}
