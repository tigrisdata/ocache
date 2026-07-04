package ring

import (
	"context"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator/gossip"
)

// testLogCapture is a simple log.Logger that captures log messages for testing.
// It implements the go-kit/log.Logger interface.
type testLogCapture struct {
	messages []map[string]interface{}
	mu       sync.Mutex
}

func (t *testLogCapture) Log(keyvals ...interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	msg := make(map[string]interface{})
	for i := 0; i < len(keyvals)-1; i += 2 {
		if key, ok := keyvals[i].(string); ok {
			msg[key] = keyvals[i+1]
		}
	}
	t.messages = append(t.messages, msg)
	return nil
}

func (t *testLogCapture) hasMessage(msgType string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range t.messages {
		if m["msg"] == msgType {
			return true
		}
	}
	return false
}

func (t *testLogCapture) getMessagesOfType(msgType string) []map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	var result []map[string]interface{}
	for _, m := range t.messages {
		if m["msg"] == msgType {
			result = append(result, m)
		}
	}
	return result
}

func TestTokenForKey_Deterministic(t *testing.T) {
	// Create a minimal RingManager just to access tokenForKey
	rm := &RingManager{}

	testCases := []struct {
		key string
	}{
		{"test-key"},
		{"another-key"},
		{"key-with-special-chars-!@#$%"},
		{""},
		{"a"},
		{"very-long-key-that-has-many-characters-in-it-to-test-hashing"},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			// Call multiple times - should always return same value
			token1 := rm.tokenForKey(tc.key)
			token2 := rm.tokenForKey(tc.key)
			token3 := rm.tokenForKey(tc.key)

			assert.Equal(t, token1, token2, "token should be deterministic")
			assert.Equal(t, token2, token3, "token should be deterministic")

			// Verify it matches direct FNV-1a computation
			h := fnv.New32a()
			_, _ = h.Write([]byte(tc.key))
			expectedToken := h.Sum32()

			assert.Equal(t, expectedToken, token1, "token should match FNV-1a hash")
		})
	}
}

func TestTokenForKey_DifferentKeys(t *testing.T) {
	rm := &RingManager{}

	keys := []string{
		"key1",
		"key2",
		"key3",
		"different",
		"another",
	}

	tokens := make(map[uint32]string)
	for _, key := range keys {
		token := rm.tokenForKey(key)
		if existingKey, exists := tokens[token]; exists {
			t.Errorf("collision: keys %q and %q both hash to token %d", existingKey, key, token)
		}
		tokens[token] = key
	}

	// All keys should produce different tokens (no collisions for this small set)
	assert.Equal(t, len(keys), len(tokens), "all keys should produce unique tokens")
}

func TestInstanceStateToNodeStatus(t *testing.T) {
	rm := &RingManager{}

	tests := []struct {
		name     string
		state    ring.InstanceState
		expected NodeStatus
	}{
		{"ACTIVE", ring.ACTIVE, NodeStatusActive},
		{"JOINING", ring.JOINING, NodeStatusJoining},
		{"PENDING", ring.PENDING, NodeStatusJoining}, // PENDING maps to Joining
		{"LEAVING", ring.LEAVING, NodeStatusLeaving},
		{"LEFT", ring.LEFT, NodeStatusDown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := rm.instanceStateToNodeStatus(tc.state)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestInstanceToNodeInfo(t *testing.T) {
	rm := &RingManager{}

	tests := []struct {
		name     string
		inst     ring.InstanceDesc
		expected NodeInfo
	}{
		{
			name: "active instance",
			inst: ring.InstanceDesc{
				Id:                  "node-1",
				Addr:                "localhost:9001",
				State:               ring.ACTIVE,
				RegisteredTimestamp: 1000,
				Tokens:              []uint32{100, 200},
			},
			expected: NodeInfo{
				ID:            "node-1",
				Address:       "localhost:9001",
				ListenAddress: "localhost:9001",
				Status:        NodeStatusActive,
				JoinedAt:      time.Unix(1000, 0),
				Weight:        1.0,
				Available:     true,
			},
		},
		{
			name: "joining instance",
			inst: ring.InstanceDesc{
				Id:                  "node-2",
				Addr:                "192.168.1.1:8080",
				State:               ring.JOINING,
				RegisteredTimestamp: 2000,
				Tokens:              []uint32{300},
			},
			expected: NodeInfo{
				ID:            "node-2",
				Address:       "192.168.1.1:8080",
				ListenAddress: "192.168.1.1:8080",
				Status:        NodeStatusJoining,
				JoinedAt:      time.Unix(2000, 0),
				Weight:        1.0,
				Available:     false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := rm.instanceToNodeInfo(&tc.inst)

			assert.Equal(t, tc.expected.ID, result.ID)
			assert.Equal(t, tc.expected.Address, result.Address)
			assert.Equal(t, tc.expected.ListenAddress, result.ListenAddress)
			assert.Equal(t, tc.expected.Status, result.Status)
			assert.Equal(t, tc.expected.JoinedAt, result.JoinedAt)
			assert.Equal(t, tc.expected.Weight, result.Weight)
			assert.Equal(t, tc.expected.Available, result.Available)
		})
	}
}

func TestRingManager_StartStop(t *testing.T) {
	// Use fresh registry to avoid duplicate registration
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	// Create memberlist KV
	nodeID := "test-node-lifecycle"
	clusterAddr := "0.0.0.0:17946" // Use high port to avoid conflicts
	seeds := []string{}            // No seeds - bootstrap node

	memberlistKV, err := gossip.NewMemberlist(nodeID, clusterAddr, seeds, logger, reg)
	require.NoError(t, err, "failed to create memberlist")

	// Create config
	cfg := LifecyclerConfig{
		InstanceID:           nodeID,
		InstanceAddr:         "localhost:19001",
		NumTokens:            128,
		ObservePeriod:        0,
		MinReadyDuration:     0,
		UnregisterOnShutdown: true,
		RingConfig: Config{
			HeartbeatPeriod:   100 * time.Millisecond,
			HeartbeatTimeout:  10 * time.Second,
			ReplicationFactor: 1,
		},
	}
	cfg.ApplyDefaults()

	ctx := context.Background()

	// Start memberlist first
	err = memberlistKV.Start(ctx)
	require.NoError(t, err, "failed to start memberlist")
	defer memberlistKV.Stop(ctx)

	// Create and start ring manager
	rm, err := NewRingManager(cfg, memberlistKV.Client(), logger, reg)
	require.NoError(t, err, "failed to create ring manager")

	err = rm.Start(ctx)
	require.NoError(t, err, "failed to start ring manager")

	// Verify basic state after start
	assert.NotNil(t, rm.ring, "ring should be initialized")
	assert.NotNil(t, rm.lifecycler, "lifecycler should be initialized")
	assert.Equal(t, nodeID, rm.localNodeID)

	// Epoch should be set (may be 0 initially before first heartbeat)
	_ = rm.GetEpoch()

	// Stop the ring manager
	err = rm.Stop(ctx)
	assert.NoError(t, err, "failed to stop ring manager")
}

func TestLogMembershipChange_DetectsJoin(t *testing.T) {
	logger := &testLogCapture{}
	rm := &RingManager{
		logger:         logger,
		epoch:          NewEpoch(),
		lastKnownNodes: map[string]ring.InstanceState{"node1": ring.ACTIVE},
	}

	// Ring descriptor with new node2
	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node1": {Id: "node1", State: ring.ACTIVE},
			"node2": {Id: "node2", State: ring.JOINING},
		},
	}

	rm.logMembershipChange(ringDesc, 12345)

	assert.True(t, logger.hasMessage("node joined"), "should log 'node joined'")
	joinMsgs := logger.getMessagesOfType("node joined")
	require.Len(t, joinMsgs, 1)
	assert.Equal(t, "node2", joinMsgs[0]["node_id"])
	assert.Equal(t, "JOINING", joinMsgs[0]["state"])
}

func TestLogMembershipChange_DetectsLeave(t *testing.T) {
	logger := &testLogCapture{}
	rm := &RingManager{
		logger: logger,
		epoch:  NewEpoch(),
		lastKnownNodes: map[string]ring.InstanceState{
			"node1": ring.ACTIVE,
			"node2": ring.ACTIVE,
		},
	}

	// Ring descriptor with node2 removed
	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node1": {Id: "node1", State: ring.ACTIVE},
		},
	}

	rm.logMembershipChange(ringDesc, 12345)

	assert.True(t, logger.hasMessage("node left"), "should log 'node left'")
	leaveMsgs := logger.getMessagesOfType("node left")
	require.Len(t, leaveMsgs, 1)
	assert.Equal(t, "node2", leaveMsgs[0]["node_id"])
}

func TestLogMembershipChange_DetectsStateChange(t *testing.T) {
	logger := &testLogCapture{}
	rm := &RingManager{
		logger: logger,
		epoch:  NewEpoch(),
		lastKnownNodes: map[string]ring.InstanceState{
			"node1": ring.ACTIVE,
			"node2": ring.JOINING,
		},
	}

	// Ring descriptor with node2 now ACTIVE
	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node1": {Id: "node1", State: ring.ACTIVE},
			"node2": {Id: "node2", State: ring.ACTIVE},
		},
	}

	rm.logMembershipChange(ringDesc, 12345)

	assert.True(t, logger.hasMessage("node state changed"), "should log 'node state changed'")
	changeMsgs := logger.getMessagesOfType("node state changed")
	require.Len(t, changeMsgs, 1)
	assert.Equal(t, "node2", changeMsgs[0]["node_id"])
	assert.Equal(t, "JOINING", changeMsgs[0]["old_state"])
	assert.Equal(t, "ACTIVE", changeMsgs[0]["new_state"])
}

func TestLogMembershipChange_UpdatesLastKnownNodes(t *testing.T) {
	logger := &testLogCapture{}
	rm := &RingManager{
		logger:         logger,
		epoch:          NewEpoch(),
		lastKnownNodes: map[string]ring.InstanceState{},
	}

	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node1": {Id: "node1", State: ring.ACTIVE},
			"node2": {Id: "node2", State: ring.JOINING},
		},
	}

	rm.logMembershipChange(ringDesc, 12345)

	assert.Len(t, rm.lastKnownNodes, 2)
	assert.Equal(t, ring.ACTIVE, rm.lastKnownNodes["node1"])
	assert.Equal(t, ring.JOINING, rm.lastKnownNodes["node2"])
}

func TestLogMembershipChange_LogsEpochUpdate(t *testing.T) {
	logger := &testLogCapture{}
	rm := &RingManager{
		logger:         logger,
		epoch:          NewEpoch(),
		lastKnownNodes: map[string]ring.InstanceState{},
	}

	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node1": {Id: "node1", State: ring.ACTIVE},
		},
	}

	rm.logMembershipChange(ringDesc, 12345)

	assert.True(t, logger.hasMessage("ring epoch updated"), "should log 'ring epoch updated'")
	epochMsgs := logger.getMessagesOfType("ring epoch updated")
	require.Len(t, epochMsgs, 1)
	assert.Equal(t, uint64(12345), epochMsgs[0]["epoch"])
	assert.Equal(t, 1, epochMsgs[0]["node_count"])
}

func TestReadinessGate_HoldsJoiningUntilMarkReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	nodeID := "test-node-readiness-gate"
	clusterAddr := "0.0.0.0:17952"
	seeds := []string{}

	memberlistKV, err := gossip.NewMemberlist(nodeID, clusterAddr, seeds, logger, reg)
	require.NoError(t, err, "failed to create memberlist")

	cfg := LifecyclerConfig{
		InstanceID:           nodeID,
		InstanceAddr:         "localhost:19052",
		NumTokens:            128,
		UnregisterOnShutdown: true,
		RingConfig: Config{
			HeartbeatPeriod:   100 * time.Millisecond,
			HeartbeatTimeout:  10 * time.Second,
			ReplicationFactor: 1,
		},
	}
	cfg.ApplyDefaults()

	ctx := context.Background()
	err = memberlistKV.Start(ctx)
	require.NoError(t, err)
	defer memberlistKV.Stop(ctx)

	rm, err := NewRingManager(cfg, memberlistKV.Client(), logger, reg)
	require.NoError(t, err)

	err = rm.Start(ctx)
	require.NoError(t, err)
	defer rm.Stop(ctx)

	// Without MarkReady the node must stay out of ACTIVE even though tokens are
	// assigned quickly. Wait well past the token-assignment window and confirm it
	// is still JOINING (not routable), so peers won't flood a still-booting node.
	time.Sleep(1 * time.Second)
	assert.NotEqual(t, ring.ACTIVE, rm.GetState(), "node must not be ACTIVE before MarkReady")
	assert.False(t, rm.IsReady(), "node must not report ready before MarkReady")

	// Releasing the gate lets it transition to ACTIVE.
	rm.MarkReady()

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	require.NoError(t, rm.WaitReady(waitCtx), "node should reach ACTIVE after MarkReady")
	assert.Equal(t, ring.ACTIVE, rm.GetState())

	// MarkReady is idempotent - a second call must not panic or block.
	rm.MarkReady()
}

func TestAnnounceLeaving_TransitionsToLeavingState(t *testing.T) {
	// Use fresh registry to avoid duplicate registration
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	// Create memberlist KV
	nodeID := "test-node-announce-leaving"
	clusterAddr := "0.0.0.0:17950"
	seeds := []string{}

	memberlistKV, err := gossip.NewMemberlist(nodeID, clusterAddr, seeds, logger, reg)
	require.NoError(t, err, "failed to create memberlist")

	cfg := LifecyclerConfig{
		InstanceID:           nodeID,
		InstanceAddr:         "localhost:19050",
		NumTokens:            128,
		ObservePeriod:        0,
		MinReadyDuration:     0,
		UnregisterOnShutdown: true,
		RingConfig: Config{
			HeartbeatPeriod:   100 * time.Millisecond,
			HeartbeatTimeout:  10 * time.Second,
			ReplicationFactor: 1,
		},
	}
	cfg.ApplyDefaults()

	ctx := context.Background()

	// Start memberlist
	err = memberlistKV.Start(ctx)
	require.NoError(t, err)
	defer memberlistKV.Stop(ctx)

	// Create and start ring manager
	rm, err := NewRingManager(cfg, memberlistKV.Client(), logger, reg)
	require.NoError(t, err)

	err = rm.Start(ctx)
	require.NoError(t, err)

	// Release the readiness gate so the node can transition JOINING->ACTIVE.
	// In production this is called once storage has booted and the gRPC server
	// is listening (issue #164).
	rm.MarkReady()

	// Wait for ACTIVE state with extended timeout for test stability
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	err = rm.WaitReady(waitCtx)
	require.NoError(t, err)
	assert.Equal(t, ring.ACTIVE, rm.GetState())

	// Give the ring some time to stabilize before calling AnnounceLeaving
	// This is needed because ChangeState requires CAS operations which can
	// race with background ring updates in test environments
	time.Sleep(200 * time.Millisecond)

	// Call AnnounceLeaving with extended timeout for test
	announceCtx, announceCancel := context.WithTimeout(ctx, 10*time.Second)
	defer announceCancel()
	err = rm.AnnounceLeaving(announceCtx)
	require.NoError(t, err)

	// Verify state is now LEAVING
	assert.Equal(t, ring.LEAVING, rm.GetState())

	// Cleanup
	err = rm.Stop(ctx)
	assert.NoError(t, err)
}
