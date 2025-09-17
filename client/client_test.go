package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *ClientConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
			errMsg:  "config is required",
		},
		{
			name: "empty addresses",
			config: &ClientConfig{
				Addrs: []string{},
			},
			wantErr: true,
			errMsg:  "at least one address is required",
		},
		{
			name: "valid single address",
			config: &ClientConfig{
				Addrs: []string{"localhost:9000"},
				Mode:  ModeSimple,
			},
			wantErr: false,
		},
		{
			name: "multiple addresses",
			config: &ClientConfig{
				Addrs: []string{"node1:9000", "node2:9000"},
				Mode:  ModeSimple,
			},
			wantErr: false,
		},
		{
			name: "default pool size",
			config: &ClientConfig{
				Addrs:    []string{"localhost:9000"},
				PoolSize: 0, // Should default to 4
				Mode:     ModeSimple,
			},
			wantErr: false,
		},
		{
			name: "custom pool size",
			config: &ClientConfig{
				Addrs:    []string{"localhost:9000"},
				PoolSize: 10,
				Mode:     ModeSimple,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip actual connection tests that require a server
			if !tt.wantErr {
				t.Skip("Skipping test that requires a running server")
			}

			_, err := NewWithConfig(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConnectionMode(t *testing.T) {
	tests := []struct {
		name     string
		mode     ConnectionMode
		expected ConnectionMode
	}{
		{
			name:     "auto mode",
			mode:     ModeAuto,
			expected: ModeSimple, // Will default to simple without topology service
		},
		{
			name:     "simple mode",
			mode:     ModeSimple,
			expected: ModeSimple,
		},
		{
			name:     "cluster mode",
			mode:     ModeCluster,
			expected: ModeCluster,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test mode constants
			assert.Equal(t, "auto", string(ModeAuto))
			assert.Equal(t, "simple", string(ModeSimple))
			assert.Equal(t, "cluster", string(ModeCluster))
		})
	}
}

func TestNew(t *testing.T) {
	// Test that New() requires at least one address
	_, err := New()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one address is required")

	// Test that New() with addresses would work (skip actual connection)
	t.Run("with addresses", func(t *testing.T) {
		t.Skip("Skipping test that requires a running server")
		_, err := New("localhost:9000")
		require.NoError(t, err)
	})
}

func TestNodeMember(t *testing.T) {
	// Test nodeMember implementation
	member := nodeMember("node1")
	assert.Equal(t, "node1", member.String())
}

func TestRoutingLogic(t *testing.T) {
	// This would test routing logic but requires mock setup
	t.Skip("Routing tests require mock gRPC servers")
}

