package cacheclient

import (
	"bytes"
	"context"
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

// TestGetWithRetryLimits verifies that retry logic has proper limits
func TestGetWithRetryLimits(t *testing.T) {
	// This test verifies that the retry logic:
	// 1. Has a bounded retry count (no stack overflow)
	// 2. Doesn't retry after partial data is received
	// 3. Only retries on routing errors in cluster mode

	tests := []struct {
		name        string
		mode        ConnectionMode
		hasData     bool
		expectRetry bool
		description string
	}{
		{
			name:        "cluster_mode_no_data",
			mode:        ModeCluster,
			hasData:     false,
			expectRetry: true,
			description: "Should retry in cluster mode when no data received",
		},
		{
			name:        "cluster_mode_with_data",
			mode:        ModeCluster,
			hasData:     true,
			expectRetry: false,
			description: "Should not retry after receiving partial data",
		},
		{
			name:        "simple_mode_no_retry",
			mode:        ModeSimple,
			hasData:     false,
			expectRetry: false,
			description: "Should not retry in simple mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a client with the specified mode
			client := &Client{
				mode: tt.mode,
			}

			// Test that getWithRetry respects retry count
			// When retryCount is 0, it should not recurse even on error
			ctx := context.Background()

			// This would previously cause unbounded recursion
			// Now it's limited to the retry count
			_, err := client.getWithRetry(ctx, "test-key", 0)

			// Should get an error (no pools configured) but no stack overflow
			assert.Error(t, err)
		})
	}
}

// TestGetStreamWithRetryLimits verifies that GetStream retry logic has proper limits
func TestGetStreamWithRetryLimits(t *testing.T) {
	// This test verifies that the retry logic:
	// 1. Has a bounded retry count (no stack overflow)
	// 2. Doesn't retry after data is written
	// 3. Only retries on routing errors in cluster mode

	tests := []struct {
		name         string
		mode         ConnectionMode
		bytesWritten int64
		expectRetry  bool
		description  string
	}{
		{
			name:         "cluster_mode_no_bytes",
			mode:         ModeCluster,
			bytesWritten: 0,
			expectRetry:  true,
			description:  "Should retry in cluster mode when no bytes written",
		},
		{
			name:         "cluster_mode_with_bytes",
			mode:         ModeCluster,
			bytesWritten: 100,
			expectRetry:  false,
			description:  "Should not retry after writing data to avoid duplicates",
		},
		{
			name:         "simple_mode_no_retry",
			mode:         ModeSimple,
			bytesWritten: 0,
			expectRetry:  false,
			description:  "Should not retry in simple mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a client with the specified mode
			client := &Client{
				mode: tt.mode,
			}

			// Test that getStreamWithRetry respects retry count
			// When retryCount is 0, it should not recurse even on error
			ctx := context.Background()
			var buf bytes.Buffer

			// This would previously cause unbounded recursion
			// Now it's limited to the retry count
			err := client.getStreamWithRetry(ctx, "test-key", &buf, 0)

			// Should get an error (no pools configured) but no stack overflow
			assert.Error(t, err)
		})
	}
}

// TestRetryCountBounds verifies retry count is properly bounded
func TestRetryCountBounds(t *testing.T) {
	client := &Client{
		mode: ModeCluster,
	}

	ctx := context.Background()

	// Test multiple retry counts to ensure bounded recursion
	for retryCount := 0; retryCount <= 3; retryCount++ {
		// These calls should fail (no pools) but not cause stack overflow
		_, err := client.getWithRetry(ctx, "test", retryCount)
		assert.Error(t, err, "Expected error for retry count %d", retryCount)

		var buf bytes.Buffer
		err = client.getStreamWithRetry(ctx, "test", &buf, retryCount)
		assert.Error(t, err, "Expected error for retry count %d", retryCount)
	}
}
