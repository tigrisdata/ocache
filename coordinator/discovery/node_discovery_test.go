package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticNodeDiscovery(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []string
		expectedNodes []string
		wantErr       bool
		errorContains string
	}{
		{
			name:          "valid static nodes",
			nodes:         []string{"localhost:7000", "localhost:7001", "192.168.1.1:7002"},
			expectedNodes: []string{"localhost:7000", "localhost:7001", "192.168.1.1:7002"},
			wantErr:       false,
		},
		{
			name:          "empty nodes",
			nodes:         []string{},
			expectedNodes: []string{},
			wantErr:       false,
		},
		{
			name:          "invalid node address",
			nodes:         []string{"localhost:7000", "invalid-node"},
			wantErr:       true,
			errorContains: "invalid node address",
		},
		{
			name:          "node with invalid port",
			nodes:         []string{"localhost:99999"},
			wantErr:       true,
			errorContains: "port must be between",
		},
		{
			name:          "filters empty strings",
			nodes:         []string{"localhost:7000", "", "localhost:7001"},
			expectedNodes: []string{"localhost:7000", "localhost:7001"},
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery, err := NewStaticNodeDiscovery(tt.nodes)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, discovery)

			// Test Mode
			assert.Equal(t, NodeDiscoveryStatic, discovery.Mode())

			// Test NeedsRefresh
			assert.False(t, discovery.NeedsRefresh())

			// Test RefreshInterval
			assert.Equal(t, time.Duration(0), discovery.RefreshInterval())

			// Test Resolve
			ctx := context.Background()
			resolvedNodes, err := discovery.Resolve(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNodes, resolvedNodes)

			// Test String
			str := discovery.String()
			assert.Contains(t, str, "StaticNodeDiscovery")

			// Resolve multiple times should return same results
			for i := 0; i < 3; i++ {
				nodes2, err := discovery.Resolve(ctx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedNodes, nodes2)
			}
		})
	}
}

func TestDNSNodeDiscovery(t *testing.T) {
	tests := []struct {
		name            string
		dnsName         string
		port            string
		refreshInterval time.Duration
		wantErr         bool
		wantResolveErr  bool
		errorContains   string
	}{
		{
			name:            "valid DNS name with port",
			dnsName:         "localhost",
			port:            "7000",
			refreshInterval: 10 * time.Second,
			wantErr:         false,
			wantResolveErr:  false,
		},
		{
			name:            "valid DNS name with default port",
			dnsName:         "localhost",
			port:            "",
			refreshInterval: 0, // Should use default
			wantErr:         true,
			errorContains:   "port cannot be empty",
			wantResolveErr:  false,
		},
		{
			name:            "empty DNS name",
			dnsName:         "",
			port:            "7000",
			refreshInterval: 10 * time.Second,
			wantErr:         true,
			errorContains:   "DNS name cannot be empty",
			wantResolveErr:  false,
		},
		{
			name:            "invalid DNS name",
			dnsName:         "invalid@name",
			port:            "7000",
			refreshInterval: 10 * time.Second,
			wantErr:         true,
			errorContains:   "invalid DNS name",
			wantResolveErr:  false,
		},
		{
			name:            "invalid port",
			dnsName:         "localhost",
			port:            "999999",
			refreshInterval: 10 * time.Second,
			wantErr:         true,
			errorContains:   "invalid port",
			wantResolveErr:  false,
		},
		{
			name:            "non-existent DNS name",
			dnsName:         "non-existent-domain-12345.invalid",
			port:            "7000",
			refreshInterval: 10 * time.Second,
			wantErr:         false,
			errorContains:   "failed to resolve DNS name",
			wantResolveErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery, err := NewDNSNodeDiscovery(tt.dnsName, tt.port, tt.refreshInterval)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, discovery)

			// Test Mode
			assert.Equal(t, NodeDiscoveryDNS, discovery.Mode())

			// Test NeedsRefresh
			assert.True(t, discovery.NeedsRefresh())

			// Test RefreshInterval
			expectedInterval := tt.refreshInterval
			if expectedInterval <= 0 {
				expectedInterval = DefaultDNSRefreshInterval
			}
			assert.Equal(t, expectedInterval, discovery.RefreshInterval())

			// Test Resolve
			ctx := context.Background()
			resolvedNodes, err := discovery.Resolve(ctx)

			// Test Resolve error
			if tt.wantResolveErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				return
			}

			// Test Resolve success
			require.NoError(t, err)
			assert.Greater(t, len(resolvedNodes), 0, "Should resolve at least one address")

			// Check that resolved addresses have the correct port
			for _, node := range resolvedNodes {
				_, port, err := net.SplitHostPort(node)
				require.NoError(t, err)
				assert.Equal(t, tt.port, port)
			}

			// Test String
			str := discovery.String()
			assert.Contains(t, str, "DNSNodeDiscovery")
			assert.Contains(t, str, tt.dnsName)

			// Test caching on resolve failure (simulate by cancelling context)
			cancelCtx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Should return cached results
			cachedNodes, err := discovery.Resolve(cancelCtx)
			if err == nil {
				// If we got cached results, they should match
				assert.Equal(t, resolvedNodes, cachedNodes)
			}
		})
	}
}

func TestCreateNodeDiscovery(t *testing.T) {
	tests := []struct {
		name         string
		nodes        []string
		expectedMode NodeDiscoveryMode
		wantErr      bool
	}{
		{
			name:         "empty nodes creates static discovery",
			nodes:        []string{},
			expectedMode: NodeDiscoveryStatic,
			wantErr:      false,
		},
		{
			name:         "single static node with port",
			nodes:        []string{"192.168.1.1:7000"},
			expectedMode: NodeDiscoveryStatic,
			wantErr:      false,
		},
		{
			name:         "multiple static nodes",
			nodes:        []string{"node1:7000", "node2:7000", "node3:7000"},
			expectedMode: NodeDiscoveryStatic,
			wantErr:      false,
		},
		{
			name:         "single hostname with port (not service name)",
			nodes:        []string{"mynode:7000"},
			expectedMode: NodeDiscoveryStatic,
			wantErr:      false,
		},
		{
			name:    "invalid node address",
			nodes:   []string{"invalid:node:address"},
			wantErr: true,
		},
		{
			name:         "single DNS name without port",
			nodes:        []string{"localhost"},
			expectedMode: NodeDiscoveryDNS,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery, err := CreateNodeDiscovery(tt.nodes, 30*time.Second)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, discovery)
			assert.Equal(t, tt.expectedMode, discovery.Mode())
		})
	}
}

func TestDiffNodes(t *testing.T) {
	tests := []struct {
		name            string
		old             []string
		new             []string
		expectedAdded   []string
		expectedRemoved []string
	}{
		{
			name:            "no changes",
			old:             []string{"node1:7000", "node2:7000"},
			new:             []string{"node1:7000", "node2:7000"},
			expectedAdded:   []string{},
			expectedRemoved: []string{},
		},
		{
			name:            "nodes added",
			old:             []string{"node1:7000"},
			new:             []string{"node1:7000", "node2:7000", "node3:7000"},
			expectedAdded:   []string{"node2:7000", "node3:7000"},
			expectedRemoved: []string{},
		},
		{
			name:            "nodes removed",
			old:             []string{"node1:7000", "node2:7000", "node3:7000"},
			new:             []string{"node1:7000"},
			expectedAdded:   []string{},
			expectedRemoved: []string{"node2:7000", "node3:7000"},
		},
		{
			name:            "nodes added and removed",
			old:             []string{"node1:7000", "node2:7000"},
			new:             []string{"node2:7000", "node3:7000"},
			expectedAdded:   []string{"node3:7000"},
			expectedRemoved: []string{"node1:7000"},
		},
		{
			name:            "from empty",
			old:             []string{},
			new:             []string{"node1:7000", "node2:7000"},
			expectedAdded:   []string{"node1:7000", "node2:7000"},
			expectedRemoved: []string{},
		},
		{
			name:            "to empty",
			old:             []string{"node1:7000", "node2:7000"},
			new:             []string{},
			expectedAdded:   []string{},
			expectedRemoved: []string{"node1:7000", "node2:7000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added, removed := DiffNodes(tt.old, tt.new)

			// Sort for consistent comparison
			assert.ElementsMatch(t, tt.expectedAdded, added, "Added nodes mismatch")
			assert.ElementsMatch(t, tt.expectedRemoved, removed, "Removed nodes mismatch")
		})
	}
}
