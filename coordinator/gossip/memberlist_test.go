package gossip

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemberlistConfig_ApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    memberlistConfig
		expected memberlistConfig
	}{
		{
			// Note: BindAddr and BindPort must be set by caller (e.g., NewMemberlistConfigFromCoordinator)
			// ApplyDefaults only sets gossip protocol parameters
			name:  "empty config gets gossip defaults only",
			input: memberlistConfig{},
			expected: memberlistConfig{
				GossipInterval:   DefaultGossipInterval,
				GossipNodes:      DefaultGossipNodes,
				PushPullInterval: DefaultPushPullInterval,
				LeaveTimeout:     DefaultLeaveTimeout,
			},
		},
		{
			name: "custom values are preserved",
			input: memberlistConfig{
				BindAddr:         "0.0.0.0",
				BindPort:         8000,
				GossipInterval:   500 * time.Millisecond,
				GossipNodes:      5,
				PushPullInterval: 1 * time.Minute,
				LeaveTimeout:     10 * time.Second,
			},
			expected: memberlistConfig{
				BindAddr:         "0.0.0.0",
				BindPort:         8000,
				GossipInterval:   500 * time.Millisecond,
				GossipNodes:      5,
				PushPullInterval: 1 * time.Minute,
				LeaveTimeout:     10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.input
			cfg.ApplyDefaults()

			assert.Equal(t, tt.expected.BindAddr, cfg.BindAddr)
			assert.Equal(t, tt.expected.BindPort, cfg.BindPort)
			assert.Equal(t, tt.expected.GossipInterval, cfg.GossipInterval)
			assert.Equal(t, tt.expected.GossipNodes, cfg.GossipNodes)
			assert.Equal(t, tt.expected.PushPullInterval, cfg.PushPullInterval)
			assert.Equal(t, tt.expected.LeaveTimeout, cfg.LeaveTimeout)
		})
	}
}

func TestNewMemberlistConfigFromCoordinator(t *testing.T) {
	tests := []struct {
		name        string
		nodeID      string
		clusterAddr string
		seeds       []string
		expectedCfg memberlistConfig
	}{
		{
			name:        "parses host and port correctly",
			nodeID:      "node-1",
			clusterAddr: "0.0.0.0:7946",
			seeds:       []string{"192.168.1.1:7946", "192.168.1.2:7946"},
			expectedCfg: memberlistConfig{
				NodeName:         "node-1",
				BindAddr:         "0.0.0.0",
				BindPort:         7946,
				JoinMembers:      []string{"192.168.1.1:7946", "192.168.1.2:7946"},
				GossipInterval:   DefaultGossipInterval,
				GossipNodes:      DefaultGossipNodes,
				PushPullInterval: DefaultPushPullInterval,
				LeaveTimeout:     DefaultLeaveTimeout,
			},
		},
		{
			name:        "custom port",
			nodeID:      "node-2",
			clusterAddr: "localhost:9000",
			seeds:       nil,
			expectedCfg: memberlistConfig{
				NodeName:         "node-2",
				BindAddr:         "localhost",
				BindPort:         9000,
				GossipInterval:   DefaultGossipInterval,
				GossipNodes:      DefaultGossipNodes,
				PushPullInterval: DefaultPushPullInterval,
				LeaveTimeout:     DefaultLeaveTimeout,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newMemberlistConfig(tt.nodeID, tt.clusterAddr, tt.seeds)

			assert.Equal(t, tt.expectedCfg.NodeName, cfg.NodeName)
			assert.Equal(t, tt.expectedCfg.BindAddr, cfg.BindAddr)
			assert.Equal(t, tt.expectedCfg.BindPort, cfg.BindPort)
			assert.Equal(t, []string(tt.expectedCfg.JoinMembers), []string(cfg.JoinMembers))
			assert.Equal(t, tt.expectedCfg.GossipInterval, cfg.GossipInterval)
			assert.Equal(t, tt.expectedCfg.GossipNodes, cfg.GossipNodes)
			assert.Equal(t, tt.expectedCfg.PushPullInterval, cfg.PushPullInterval)
			assert.Equal(t, tt.expectedCfg.LeaveTimeout, cfg.LeaveTimeout)
		})
	}
}

func TestMemberlistConfig_ToMemberlistConfig(t *testing.T) {
	cfg := memberlistConfig{
		JoinMembers:       []string{"node1:7946", "node2:7946"},
		AbortIfJoinFails:  true,
		BindAddr:          "0.0.0.0",
		BindPort:          7946,
		AdvertiseAddr:     "192.168.1.1",
		AdvertisePort:     7946,
		GossipInterval:    200 * time.Millisecond,
		GossipNodes:       3,
		PushPullInterval:  30 * time.Second,
		LeaveTimeout:      5 * time.Second,
		NodeName:          "test-node",
		RandomizeNodeName: false,
	}

	mlCfg := cfg.ToKVConfig()

	assert.Equal(t, []string(cfg.JoinMembers), []string(mlCfg.JoinMembers))
	assert.Equal(t, cfg.AbortIfJoinFails, mlCfg.AbortIfJoinFails)
	assert.Equal(t, cfg.BindPort, mlCfg.TCPTransport.BindPort)
	assert.Equal(t, cfg.AdvertiseAddr, mlCfg.AdvertiseAddr)
	assert.Equal(t, cfg.AdvertisePort, mlCfg.AdvertisePort)
	assert.Equal(t, cfg.GossipInterval, mlCfg.GossipInterval)
	assert.Equal(t, cfg.GossipNodes, mlCfg.GossipNodes)
	assert.Equal(t, cfg.PushPullInterval, mlCfg.PushPullInterval)
	assert.Equal(t, cfg.LeaveTimeout, mlCfg.LeaveTimeout)
	assert.Equal(t, cfg.NodeName, mlCfg.NodeName)
	assert.Equal(t, cfg.RandomizeNodeName, mlCfg.RandomizeNodeName)
}

func TestSimpleDNSProvider_Resolve_IPAddresses(t *testing.T) {
	// IP addresses should be passed through without DNS lookup
	seedAddrs := []string{"192.168.1.1:7946", "10.0.0.1:7946"}
	logger := log.NewNopLogger()
	provider := newSimpleDNSProvider(seedAddrs, logger)

	ctx := context.Background()
	err := provider.Resolve(ctx, seedAddrs)
	require.NoError(t, err)

	addrs := provider.Addresses()
	assert.Equal(t, seedAddrs, addrs)
}

func TestSimpleDNSProvider_Resolve_Localhost(t *testing.T) {
	// localhost should be resolvable via DNS
	seedAddrs := []string{"localhost:7946"}
	logger := log.NewNopLogger()
	provider := newSimpleDNSProvider(seedAddrs, logger)

	ctx := context.Background()
	err := provider.Resolve(ctx, seedAddrs)
	require.NoError(t, err)

	addrs := provider.Addresses()
	// localhost should resolve to at least one address
	assert.NotEmpty(t, addrs)

	// All resolved addresses should have the port
	for _, addr := range addrs {
		assert.Contains(t, addr, ":7946")
	}
}
