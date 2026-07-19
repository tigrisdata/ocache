// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/service"
	"github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeMetrics tracks metrics for a single cluster node
type NodeMetrics struct {
	NodeID         string
	KeysStored     atomic.Int64 // Keys stored on this node
	WritesHandled  atomic.Int64 // Put operations handled
	ReadsHandled   atomic.Int64 // Get operations handled
	DeletesHandled atomic.Int64 // Delete operations handled
	BytesWritten   atomic.Int64
	BytesRead      atomic.Int64
}

// ClusterServerNode represents a full cache server node with storage and coordinator
type ClusterServerNode struct {
	NodeID          string
	ListenAddr      string
	ClusterAddr     string
	TempDir         string
	Coordinator     *coordinator.Coordinator
	GRPCServer      *grpc.Server
	Storage         *storage.Storage
	ClusterConn     *grpc.ClientConn
	ClusterSvc      clusterpb.ClusterServiceClient
	Metrics         *NodeMetrics
	ctx             context.Context
	cancel          context.CancelFunc
	listener        net.Listener
	mu              sync.RWMutex
	stopped         bool
	serverStartedCh chan struct{}
}

// Stop stops the cluster server node
func (n *ClusterServerNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return nil
	}

	// Close the cluster client connection first
	if n.ClusterConn != nil {
		n.ClusterConn.Close()
	}

	// Stop the coordinator gracefully - this will:
	// 1. Stop ring manager (unregister from ring, broadcast leave)
	// 2. Stop memberlist KV
	// The coordinator handles its own shutdown order correctly.
	if n.Coordinator != nil {
		if err := n.Coordinator.Stop(); err != nil {
			return err
		}
	}

	// Stop gRPC server after coordinator (it might still be handling requests during unregister)
	if n.GRPCServer != nil {
		n.GRPCServer.GracefulStop()
	}

	// Close storage after gRPC server to ensure all pending operations complete
	if n.Storage != nil {
		n.Storage.Close()
	}

	// Cancel context last - this is just for cleanup, not for triggering shutdown
	if n.cancel != nil {
		n.cancel()
	}

	if n.TempDir != "" {
		os.RemoveAll(n.TempDir)
	}

	n.stopped = true
	return nil
}

// IsRunning checks if the node is running
func (n *ClusterServerNode) IsRunning() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return !n.stopped
}

// ClusterTestHarness provides a test harness for multi-node cluster testing with full servers
type ClusterTestHarness struct {
	T               *testing.T
	Nodes           map[string]*ClusterServerNode
	Client          cacheclient.CacheClient
	NodeCount       int
	Config          IntegrationTestConfig
	Metrics         *TestMetrics
	NodeMetrics     map[string]*NodeMetrics // Per-node metrics
	grpcPorts       []int                   // Dynamically allocated gRPC ports
	memberlistPorts []int                   // Dynamically allocated memberlist ports
	mu              sync.RWMutex
	stopMetrics     chan struct{}
	cleanupOnce     sync.Once // Ensures Cleanup only runs once
	clientAddrs     []string
}

// NewClusterTestHarness creates a new cluster test harness with full cache servers
func NewClusterTestHarness(t *testing.T, nodeCount int, config IntegrationTestConfig) *ClusterTestHarness {
	// Get free ports dynamically: nodeCount for gRPC + nodeCount for memberlist
	ports, err := getFreePorts(nodeCount * 2)
	if err != nil {
		t.Fatalf("Failed to get free ports: %v", err)
	}

	// First half for gRPC, second half for memberlist
	grpcPorts := ports[:nodeCount]
	memberlistPorts := ports[nodeCount:]

	t.Logf("ClusterTestHarness using gRPC ports %v, memberlist ports %v", grpcPorts, memberlistPorts)

	return &ClusterTestHarness{
		T:               t,
		Nodes:           make(map[string]*ClusterServerNode),
		NodeCount:       nodeCount,
		Config:          config,
		Metrics:         &TestMetrics{StartTime: time.Now()},
		NodeMetrics:     make(map[string]*NodeMetrics),
		grpcPorts:       grpcPorts,
		memberlistPorts: memberlistPorts,
		stopMetrics:     make(chan struct{}),
	}
}

// StartNode starts a full cache server node
func (h *ClusterTestHarness) StartNode(nodeIndex int) (*ClusterServerNode, error) {
	nodeID := fmt.Sprintf("cluster-node-%d", nodeIndex+1)
	listenPort := h.grpcPorts[nodeIndex]           // gRPC service port (cache + cluster RPCs)
	memberlistPort := h.memberlistPorts[nodeIndex] // Memberlist gossip port
	listenAddr := fmt.Sprintf("localhost:%d", listenPort)
	clusterAddr := fmt.Sprintf("0.0.0.0:%d", memberlistPort) // Memberlist requires IP, not hostname

	// Create temporary directory for this node
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("ocache-cluster-test-node-%d-*", nodeIndex))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Build seed list (memberlist addresses of all nodes)
	var seeds []string
	for i := 0; i < h.NodeCount; i++ {
		// Seeds use 127.0.0.1 so nodes can reach each other
		seedAddr := fmt.Sprintf("127.0.0.1:%d", h.memberlistPorts[i])
		seeds = append(seeds, seedAddr)
	}

	// Initialize isolated storage instance for this node
	storageConfig := &storage.StorageConfig{
		DiskPath:             tmpDir,
		TTL:                  0,
		InlineThreshold:      int(h.Config.InlineThreshold),
		CompactThreshold:     h.Config.CompactThreshold,
		SegmentSize:          h.Config.SegmentSize,
		FdCacheSize:          h.Config.FDCacheSize,
		MaxDiskUsage:         h.Config.MaxDiskUsage,
		EvictionPolicy:       h.Config.EvictionPolicy,
		CompactionThreads:    h.Config.CompactionThreads,
		MinSegmentAge:        h.Config.RecompactMinSegmentAge,
		MinSegments:          h.Config.RecompactMinSegments,
		RecompactionInterval: h.Config.RecompactionInterval,
		CleanupInterval:      h.Config.CleanupInterval,
		AccessUpdateDelay:    h.Config.AccessUpdateDelay,
	}
	stor, err := storage.NewStorageWithConfig(storageConfig)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Create coordinator config with dskit ring
	coordConfig := &coordinator.Config{
		Enabled:     true,
		MyNodeID:    nodeID,
		ClusterAddr: clusterAddr,
		ListenAddr:  listenAddr,
		Seeds:       seeds,
		DiskPath:    tmpDir,
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens:            128,                    // Fewer tokens for faster testing
			ObservePeriod:        100 * time.Millisecond, // Very fast observe for testing
			MinReadyDuration:     0,                      // No minimum ready duration for testing
			UnregisterOnShutdown: true,                   // Remove from ring on shutdown for clean departure
			RingConfig: ring.Config{
				HeartbeatPeriod:  100 * time.Millisecond, // Fast heartbeat for testing
				HeartbeatTimeout: 10 * time.Second,       // Longer timeout to account for gossip delay
			},
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	// Create coordinator
	coord, err := coordinator.New(coordConfig)
	if err != nil {
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to create coordinator: %w", err)
	}

	// Create node context
	ctx, cancel := context.WithCancel(context.Background())

	// Start coordinator
	if err := coord.Start(ctx); err != nil {
		// Even though Start failed, we need to call Stop() to cleanup
		// any resources that were successfully initialized (like the gRPC server)
		coord.Stop() // This is safe even if Start() partially failed
		cancel()
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Create node metrics
	nodeMetrics := &NodeMetrics{
		NodeID: nodeID,
	}

	// Create gRPC server with metrics interceptors and cache service
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
		grpc.UnaryInterceptor(metricsUnaryInterceptor(nodeMetrics)),
		grpc.StreamInterceptor(metricsStreamInterceptor(nodeMetrics)),
	)

	// Register cache service with node-specific storage instance
	cacheService := service.NewCacheService(coord, stor)
	pb.RegisterCacheServiceServer(grpcServer, cacheService)

	// Register ClusterService on the same gRPC server
	clusterpb.RegisterClusterServiceServer(grpcServer, coord)

	// Start gRPC server
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		coord.Stop()
		cancel()
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	// Storage is open and the peer listener is bound, so signal readiness to let
	// the node advertise ACTIVE (issue #164). Production wires this into
	// StartGRPCServer; this harness runs its own gRPC server, so it marks ready
	// itself.
	coord.MarkReady()

	// Channel to signal when server is ready
	serverStartedCh := make(chan struct{})

	// Start gRPC server in background
	go func() {
		close(serverStartedCh)
		if err := grpcServer.Serve(listener); err != nil {
			h.T.Logf("gRPC server stopped for node %s: %v", nodeID, err)
		}
	}()

	// Wait for server to start
	<-serverStartedCh
	time.Sleep(100 * time.Millisecond)

	// Create cluster client connection for topology queries (connect to ListenAddr, not ClusterAddr)
	// Use a context with timeout instead of deprecated grpc.WithTimeout
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()

	clusterConn, err := grpc.DialContext(dialCtx, listenAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcServer.Stop()
		coord.Stop()
		cancel()
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to connect to cluster service: %w", err)
	}

	node := &ClusterServerNode{
		NodeID:          nodeID,
		ListenAddr:      listenAddr,
		ClusterAddr:     clusterAddr,
		TempDir:         tmpDir,
		Coordinator:     coord,
		GRPCServer:      grpcServer,
		Storage:         stor,
		ClusterConn:     clusterConn,
		ClusterSvc:      clusterpb.NewClusterServiceClient(clusterConn),
		Metrics:         nodeMetrics,
		ctx:             ctx,
		cancel:          cancel,
		listener:        listener,
		serverStartedCh: serverStartedCh,
	}

	h.mu.Lock()
	h.Nodes[nodeID] = node
	h.NodeMetrics[nodeID] = nodeMetrics
	h.mu.Unlock()

	return node, nil
}

// StartAllNodes starts all nodes and creates a cluster client
func (h *ClusterTestHarness) StartAllNodes() error {
	for i := 0; i < h.NodeCount; i++ {
		if _, err := h.StartNode(i); err != nil {
			return fmt.Errorf("failed to start node %d: %w", i, err)
		}

		// Add delay between starts
		if i < h.NodeCount-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Wait for cluster convergence
	if err := h.WaitForConvergence(10 * time.Second); err != nil {
		return fmt.Errorf("cluster did not converge: %w", err)
	}

	// Build client addresses
	h.clientAddrs = make([]string, 0, h.NodeCount)
	for _, node := range h.Nodes {
		h.clientAddrs = append(h.clientAddrs, node.ListenAddr)
	}

	// Create cluster client
	config := &cacheclient.ClientConfig{
		Addrs: h.clientAddrs,
	}
	client, err := cacheclient.NewClusterClient(config)
	if err != nil {
		return fmt.Errorf("failed to create cluster client: %w", err)
	}
	h.Client = client

	// Start metrics collection
	h.startMetricsCollection()

	return nil
}

// StopAllNodes stops all nodes
func (h *ClusterTestHarness) StopAllNodes() {
	h.mu.Lock()
	defer h.mu.Unlock()

	var wg sync.WaitGroup
	for nodeID, node := range h.Nodes {
		wg.Add(1)
		go func(id string, n *ClusterServerNode) {
			defer wg.Done()
			if err := n.Stop(); err != nil {
				h.T.Logf("Failed to stop node %s: %v", id, err)
			}
		}(nodeID, node)
	}

	wg.Wait()
	h.Nodes = make(map[string]*ClusterServerNode)

	if h.Client != nil {
		h.Client.Close()
	}
}

// WaitForConvergence waits for topology to converge
func (h *ClusterTestHarness) WaitForConvergence(timeout time.Duration) error {
	start := time.Now()

	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("topology did not converge within %v", timeout)
		}

		if h.IsConverged() {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// IsConverged checks if all nodes have the same view of the cluster.
// With content-addressable epochs, we can simply compare epoch values across nodes -
// nodes with identical ring views will have identical epochs.
func (h *ClusterTestHarness) IsConverged() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.Nodes) == 0 {
		return false
	}

	// Count running nodes
	runningNodes := 0
	for _, node := range h.Nodes {
		if node.IsRunning() {
			runningNodes++
		}
	}

	if runningNodes == 0 {
		return false
	}

	var referenceEpoch uint64
	var referenceNodeCount int
	first := true

	for nodeID, node := range h.Nodes {
		if !node.IsRunning() {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		topology, err := node.ClusterSvc.GetClusterTopology(ctx, &clusterpb.Empty{})
		cancel()

		if err != nil {
			return false
		}

		if first {
			referenceEpoch = topology.Epoch
			referenceNodeCount = len(topology.Nodes)
			first = false
			continue
		}

		// With content-addressable epochs, same epoch = same ring view
		if topology.Epoch != referenceEpoch {
			h.T.Logf("Epoch mismatch: node %s has epoch %d, expected %d", nodeID, topology.Epoch, referenceEpoch)
			return false
		}

		// Also verify node count matches (sanity check)
		if len(topology.Nodes) != referenceNodeCount {
			h.T.Logf("Node count mismatch: node %s sees %d nodes, expected %d", nodeID, len(topology.Nodes), referenceNodeCount)
			return false
		}

		// Verify all nodes are ACTIVE (not just epoch match)
		for _, topologyNode := range topology.Nodes {
			if topologyNode.Status != clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
				h.T.Logf("Node %s sees node %s as %v (not ACTIVE)", nodeID, topologyNode.Id, topologyNode.Status)
				return false
			}
		}
	}

	return true
}

// startMetricsCollection starts background metrics collection
func (h *ClusterTestHarness) startMetricsCollection() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Metrics updated during operations
			case <-h.stopMetrics:
				return
			}
		}
	}()
}

// TestHarnessInterface implementation

// PutObject stores an object via the cluster client
func (h *ClusterTestHarness) PutObject(key string, data []byte, ttl int64) error {
	h.Metrics.TotalWrites.Add(1)
	h.Metrics.BytesWritten.Add(int64(len(data)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := h.Client.Put(ctx, key, data, ttl)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return err
	}

	// Track object type based on size
	if int64(len(data)) <= h.Config.InlineThreshold {
		h.Metrics.InlineObjects.Add(1)
	} else {
		h.Metrics.RawFileObjects.Add(1)
	}

	return nil
}

// PutObjectStream stores a large object using streaming API (for objects >128MB)
func (h *ClusterTestHarness) PutObjectStream(key string, data []byte, ttl int64) error {
	h.Metrics.TotalWrites.Add(1)
	h.Metrics.BytesWritten.Add(int64(len(data)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Longer timeout for large objects
	defer cancel()

	err := h.Client.PutStream(ctx, key, bytes.NewReader(data), ttl)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return err
	}

	// Track object type based on size
	if int64(len(data)) <= h.Config.InlineThreshold {
		h.Metrics.InlineObjects.Add(1)
	} else {
		h.Metrics.RawFileObjects.Add(1)
	}

	return nil
}

// GetObject retrieves an object via the cluster client
func (h *ClusterTestHarness) GetObject(key string) ([]byte, error) {
	h.Metrics.TotalReads.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data, err := h.Client.Get(ctx, key)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, err
	}

	h.Metrics.BytesRead.Add(int64(len(data)))
	return data, nil
}

// DeleteObject deletes an object via the cluster client
func (h *ClusterTestHarness) DeleteObject(key string) error {
	h.Metrics.TotalDeletes.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := h.Client.Delete(ctx, key)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return err
	}

	return nil
}

// List returns all keys with the given prefix
func (h *ClusterTestHarness) List(prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys, err := h.Client.List(ctx, prefix)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, err
	}
	return keys, nil
}

// ListPage returns a page of keys with pagination support
func (h *ClusterTestHarness) ListPage(prefix string, limit int, continuationToken string) (keys []string, nextToken string, hasMore bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys, nextToken, hasMore, err = h.Client.ListPage(ctx, prefix, limit, continuationToken)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, "", false, err
	}

	return keys, nextToken, hasMore, nil
}

// ListPageWithValues returns a page of key-value pairs with pagination support
func (h *ClusterTestHarness) ListPageWithValues(prefix string, limit int, continuationToken string) (entries []KeyValueEntry, nextToken string, hasMore bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientEntries, nextToken, hasMore, err := h.Client.ListPageWithValues(ctx, prefix, limit, continuationToken)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, "", false, err
	}

	entries = make([]KeyValueEntry, len(clientEntries))
	for i, e := range clientEntries {
		entries[i] = KeyValueEntry{Key: e.Key, Value: e.Value}
	}

	return entries, nextToken, hasMore, nil
}

// GetStorageStats returns aggregate storage statistics across all nodes
func (h *ClusterTestHarness) GetStorageStats() StorageStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := StorageStats{}

	for _, node := range h.Nodes {
		if !node.IsRunning() || node.Storage == nil || node.Storage.IsClosed() {
			continue
		}

		// Get list of keys
		keys, err := node.Storage.ListKeys("")
		if err == nil {
			stats.TotalKeys += len(keys)
		}

		// Count raw files
		filesDir := filepath.Join(node.TempDir, "files")
		if files, err := filepath.Glob(filepath.Join(filesDir, "*")); err == nil {
			stats.RawFileCount += len(files)
		}

		// Count segment files
		segmentDir := filepath.Join(node.TempDir, "segments")
		if files, err := filepath.Glob(filepath.Join(segmentDir, "segment_*.seg")); err == nil {
			stats.SegmentCount += len(files)
		}
	}

	return stats
}

// Cleanup cleans up the test harness
func (h *ClusterTestHarness) Cleanup() {
	h.cleanupOnce.Do(func() {
		h.Metrics.EndTime = time.Now()
		close(h.stopMetrics)
		h.StopAllNodes()
	})
}

// GetNodeForKey returns which node should own a given key based on consistent hashing
func (h *ClusterTestHarness) GetNodeForKey(key string) (*ClusterServerNode, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Use any node's coordinator to determine owner
	for _, node := range h.Nodes {
		if node.Coordinator == nil {
			continue
		}
		ringManager := node.Coordinator.GetRing()
		if ringManager == nil {
			continue
		}
		primaryNode, err := ringManager.GetPrimaryNode(key)
		if err != nil {
			return nil, err
		}
		return h.Nodes[primaryNode.ID], nil
	}
	return nil, fmt.Errorf("no nodes available")
}

// VerifyKeyDistribution checks if keys are distributed correctly across nodes
// Returns map[nodeID][]keys showing expected distribution
func (h *ClusterTestHarness) VerifyKeyDistribution(keys []string) (map[string][]string, error) {
	distribution := make(map[string][]string)

	for _, key := range keys {
		node, err := h.GetNodeForKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get node for key %s: %w", key, err)
		}
		distribution[node.NodeID] = append(distribution[node.NodeID], key)
	}

	return distribution, nil
}

// GetDistributionStats returns statistics about workload distribution across nodes
func (h *ClusterTestHarness) GetDistributionStats() *DistributionStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := &DistributionStats{
		NodeCount: h.NodeCount,
		PerNode:   make(map[string]NodeDistribution),
	}

	for nodeID, metrics := range h.NodeMetrics {
		// Count actual keys stored on this node
		var keyCount int64
		// Check both IsRunning() and Storage.IsClosed() to ensure the node's storage is safe to access
		if node, exists := h.Nodes[nodeID]; exists && node.IsRunning() && node.Storage != nil && !node.Storage.IsClosed() {
			keys, err := node.Storage.ListKeys("")
			if err == nil {
				keyCount = int64(len(keys))
			}
		}

		stats.PerNode[nodeID] = NodeDistribution{
			KeyCount:     keyCount,
			WriteCount:   metrics.WritesHandled.Load(),
			ReadCount:    metrics.ReadsHandled.Load(),
			DeleteCount:  metrics.DeletesHandled.Load(),
			BytesWritten: metrics.BytesWritten.Load(),
			BytesRead:    metrics.BytesRead.Load(),
		}
	}

	// Calculate balance metrics
	stats.CalculateBalance()
	return stats
}

// GetTokenDistribution shows how tokens are distributed across nodes.
// Returns map[nodeID]tokenCount showing the number of tokens each node owns.
func (h *ClusterTestHarness) GetTokenDistribution() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	distribution := make(map[string]int)

	// Use first node's coordinator to get token distribution
	for _, node := range h.Nodes {
		if node.Coordinator == nil {
			continue
		}
		ringManager := node.Coordinator.GetRing()
		if ringManager == nil {
			continue
		}

		// Get actual token assignments from the ring
		nodeTokens := ringManager.GetNodeTokens()
		for nodeID, tokens := range nodeTokens {
			distribution[nodeID] = len(tokens)
		}

		// Include nodes with 0 tokens (not yet active)
		allNodes := ringManager.GetAllNodes()
		for _, nodeInfo := range allNodes {
			if _, exists := distribution[nodeInfo.ID]; !exists {
				distribution[nodeInfo.ID] = 0
			}
		}

		break // Only need one node's view
	}

	return distribution
}

// PrintMetrics prints test metrics
func (h *ClusterTestHarness) PrintMetrics() {
	endTime := h.Metrics.EndTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	duration := endTime.Sub(h.Metrics.StartTime)
	fmt.Printf("\n=== Cluster Integration Test Metrics ===\n")
	fmt.Printf("Nodes: %d\n", h.NodeCount)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Total Writes: %d\n", h.Metrics.TotalWrites.Load())
	fmt.Printf("Total Reads: %d\n", h.Metrics.TotalReads.Load())
	fmt.Printf("Total Deletes: %d\n", h.Metrics.TotalDeletes.Load())
	fmt.Printf("Bytes Written: %d\n", h.Metrics.BytesWritten.Load())
	fmt.Printf("Bytes Read: %d\n", h.Metrics.BytesRead.Load())
	fmt.Printf("Inline Objects: %d\n", h.Metrics.InlineObjects.Load())
	fmt.Printf("Raw File Objects: %d\n", h.Metrics.RawFileObjects.Load())
	fmt.Printf("Error Count: %d\n", h.Metrics.ErrorCount.Load())

	// Print per-node distribution
	stats := h.GetDistributionStats()
	if len(stats.PerNode) > 0 {
		fmt.Printf("\n=== Per-Node Distribution ===\n")
		for nodeID, dist := range stats.PerNode {
			fmt.Printf("Node %s:\n", nodeID)
			fmt.Printf("  Keys Stored: %d\n", dist.KeyCount)
			fmt.Printf("  Writes: %d\n", dist.WriteCount)
			fmt.Printf("  Reads: %d\n", dist.ReadCount)
			fmt.Printf("  Deletes: %d\n", dist.DeleteCount)
			fmt.Printf("  Bytes Written: %d\n", dist.BytesWritten)
			fmt.Printf("  Bytes Read: %d\n", dist.BytesRead)
		}

		fmt.Printf("\n=== Balance Metrics ===\n")
		fmt.Printf("Balance Score: %.2f/100\n", stats.BalanceScore)
		fmt.Printf("Key Count Std Dev: %.2f\n", stats.KeyCountStdDev)
		fmt.Printf("Max/Min Key Ratio: %.2fx\n", stats.MaxMinKeyRatio)
		fmt.Printf("Write Count Std Dev: %.2f\n", stats.WriteCountStdDev)
	}

	fmt.Printf("========================================\n")
}

// GetTempDir returns a temporary directory (uses first node's temp dir)
func (h *ClusterTestHarness) GetTempDir() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, node := range h.Nodes {
		if node.IsRunning() {
			return node.TempDir
		}
	}
	return ""
}

// metricsUnaryInterceptor tracks metrics for unary RPC calls
func metricsUnaryInterceptor(metrics *NodeMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)

		// Only track metrics if operation succeeded
		if err == nil && metrics != nil {
			switch info.FullMethod {
			case "/cache.CacheService/PutObject":
				if putReq, ok := req.(*pb.PutRequest); ok {
					metrics.WritesHandled.Add(1)
					metrics.BytesWritten.Add(int64(len(putReq.Data)))
					metrics.KeysStored.Add(1)
				}
			case "/cache.CacheService/Delete":
				metrics.DeletesHandled.Add(1)
			}
		}

		return resp, err
	}
}

// metricsStreamInterceptor tracks metrics for streaming RPC calls
func metricsStreamInterceptor(metrics *NodeMetrics) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Wrap the stream to intercept messages for byte/key counting
		wrapped := &metricsServerStream{
			ServerStream: ss,
			metrics:      metrics,
			method:       info.FullMethod,
		}

		// Execute the handler
		err := handler(srv, wrapped)

		// Only track operation counts if the handler succeeded
		if err == nil && metrics != nil {
			switch info.FullMethod {
			case "/cache.CacheService/Get":
				metrics.ReadsHandled.Add(1)
			case "/cache.CacheService/Put":
				metrics.WritesHandled.Add(1)
				// Track key storage for successful Put operations
				if wrapped.keyTracked {
					metrics.KeysStored.Add(1)
				}
			}
		}

		return err
	}
}

// metricsServerStream wraps grpc.ServerStream to track metrics
type metricsServerStream struct {
	grpc.ServerStream
	metrics    *NodeMetrics
	method     string
	keyTracked bool // Track if we've already counted the key for this stream
}

func (s *metricsServerStream) SendMsg(m interface{}) error {
	// Track Get (download) metrics
	if s.method == "/cache.CacheService/Get" {
		if getResp, ok := m.(*pb.GetResponse); ok && s.metrics != nil {
			s.metrics.BytesRead.Add(int64(len(getResp.Data)))
		}
	}
	return s.ServerStream.SendMsg(m)
}

func (s *metricsServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)

	// Track Put (upload) byte counts
	if s.method == "/cache.CacheService/Put" {
		if putReq, ok := m.(*pb.PutRequest); ok && s.metrics != nil && err == nil {
			s.metrics.BytesWritten.Add(int64(len(putReq.Data)))
			// Mark that we've seen a key (for tracking in the interceptor on success)
			if putReq.Key != "" && !s.keyTracked {
				s.keyTracked = true
			}
		}
	}

	return err
}
