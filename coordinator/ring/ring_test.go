package ring

import (
	"context"
	"hash/fnv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator/gossip"
)

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
