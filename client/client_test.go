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

func TestRoutingLogic(t *testing.T) {
	// This would test routing logic but requires mock setup
	t.Skip("Routing tests require mock gRPC servers")
}
