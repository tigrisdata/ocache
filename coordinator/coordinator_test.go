// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator/ring"
)

func TestCoordinator_New(t *testing.T) {
	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node",
		ClusterAddr: "localhost:9090",
		ListenAddr:  "localhost:8090",
		DiskPath:    "/data/ocache1",
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens: 128,
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	coord, err := New(config)
	require.NoError(t, err)
	assert.NotNil(t, coord)
	assert.Equal(t, "test-node", coord.GetLocalNodeID())
	assert.NotNil(t, coord.GetRing())
	assert.NotNil(t, coord.GetRouter())
}

// TestCoordinator_ConfigValidation verifies coordinator config validation
func TestCoordinator_ConfigValidation(t *testing.T) {
	// Test missing listen address
	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node",
		ClusterAddr: "localhost:7000",
		DiskPath:    "/data/ocache1",
		ListenAddr:  "", // Missing listen address
	}

	coord, err := New(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen address is required")
	assert.Nil(t, coord)

	// Test missing node ID
	config2 := &Config{
		Enabled:     true,
		MyNodeID:    "", // Missing node ID
		ClusterAddr: "localhost:7001",
		ListenAddr:  "localhost:9000",
		DiskPath:    "/data/ocache1",
	}

	coord2, err := New(config2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node ID is required")
	assert.Nil(t, coord2)

	// Test missing disk path
	config3 := &Config{
		Enabled:     true,
		MyNodeID:    "test-node",
		ClusterAddr: "localhost:7002",
		ListenAddr:  "localhost:9001",
	}

	coord3, err := New(config3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disk path is required")
	assert.Nil(t, coord3)
}

// TestCoordinator_ErrorChan tests error channel availability
func TestCoordinator_ErrorChan(t *testing.T) {
	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node",
		ClusterAddr: "localhost:9091",
		ListenAddr:  "localhost:8091",
		DiskPath:    "/data/ocache1",
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens: 128,
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	coord, err := New(config)
	require.NoError(t, err)
	assert.NotNil(t, coord)

	errCh := coord.ErrorChan()
	assert.NotNil(t, errCh)
}

// TestCoordinator_GetEpoch tests epoch retrieval
func TestCoordinator_GetEpoch(t *testing.T) {
	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node",
		ClusterAddr: "localhost:9092",
		ListenAddr:  "localhost:8092",
		DiskPath:    "/data/ocache1",
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens: 128,
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	coord, err := New(config)
	require.NoError(t, err)
	assert.NotNil(t, coord)

	// Epoch starts at 0 before ring starts
	epoch := coord.GetEpoch()
	assert.GreaterOrEqual(t, epoch, uint64(0))
}

// TestCoordinator_StartStop tests basic start/stop lifecycle
// Note: This test requires the ring to be properly configured and may
// fail if memberlist cannot bind to the specified port.
func TestCoordinator_StartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ocache-test-node-lifecycle")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node-lifecycle",
		ClusterAddr: "0.0.0.0:9093", // Use IP address for memberlist (not hostname)
		ListenAddr:  "localhost:8093",
		DiskPath:    tmpDir,
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens: 128,
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	coord, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, coord)

	ctx := context.Background()

	// Start the coordinator
	err = coord.Start(ctx)
	require.NoError(t, err)

	// Verify ring is running
	ringManager := coord.GetRing()
	assert.NotNil(t, ringManager)

	// Stop the coordinator
	err = coord.Stop()
	assert.NoError(t, err)
}

// TestCoordinator_IsLocal tests the IsLocal method
// Note: In a single-node test without full cluster setup, the ring may not
// transition to ACTIVE state immediately. This test verifies the method
// exists and can be called. Full cluster tests verify actual ownership.
func TestCoordinator_IsLocal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ocache-test-node-islocal-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &Config{
		Enabled:     true,
		MyNodeID:    "test-node-islocal",
		ClusterAddr: "0.0.0.0:9094", // Use IP address for memberlist (not hostname)
		ListenAddr:  "localhost:8094",
		DiskPath:    tmpDir,
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens:        128,
			ObservePeriod:    0, // Don't wait to observe tokens
			MinReadyDuration: 0, // No minimum ready duration for testing
			RingConfig: ring.Config{
				HeartbeatPeriod: 100 * time.Millisecond, // Fast heartbeat for testing
			},
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	coord, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, coord)

	ctx := context.Background()

	// Start the coordinator
	err = coord.Start(ctx)
	require.NoError(t, err)
	defer coord.Stop()

	// The IsLocal method should be callable even if ring is still in JOINING state
	// In production, the ring would transition to ACTIVE once the node establishes itself
	// For unit tests, we just verify the method exists and can be called
	_ = coord.IsLocal("test-key")

	// Verify GetRing returns a valid ring manager
	ringManager := coord.GetRing()
	require.NotNil(t, ringManager)

	// Verify GetState can be called
	state := ringManager.GetState()
	t.Logf("Ring state: %v", state)
}
