package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/common/hash"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	if n.cancel != nil {
		n.cancel()
	}

	if n.GRPCServer != nil {
		n.GRPCServer.GracefulStop()
	}

	if n.Coordinator != nil {
		if err := n.Coordinator.Stop(); err != nil {
			return err
		}
	}

	if n.ClusterConn != nil {
		n.ClusterConn.Close()
	}

	if n.Storage != nil {
		n.Storage.Close()
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
	T           *testing.T
	Nodes       map[string]*ClusterServerNode
	Client      cacheclient.CacheClient
	BasePort    int
	NodeCount   int
	Config      IntegrationTestConfig
	Metrics     *TestMetrics
	mu          sync.RWMutex
	stopMetrics chan struct{}
	clientAddrs []string
}

// NewClusterTestHarness creates a new cluster test harness with full cache servers
func NewClusterTestHarness(t *testing.T, nodeCount int, config IntegrationTestConfig) *ClusterTestHarness {
	return &ClusterTestHarness{
		T:           t,
		Nodes:       make(map[string]*ClusterServerNode),
		BasePort:    30000, // Use separate port range to avoid conflicts (coordinator uses 27000-28002)
		NodeCount:   nodeCount,
		Config:      config,
		Metrics:     &TestMetrics{StartTime: time.Now()},
		stopMetrics: make(chan struct{}),
	}
}

// StartNode starts a full cache server node
func (h *ClusterTestHarness) StartNode(nodeIndex int) (*ClusterServerNode, error) {
	nodeID := fmt.Sprintf("cluster-node-%d", nodeIndex+1)
	listenPort := h.BasePort + nodeIndex
	clusterPort := h.BasePort + 1000 + nodeIndex
	listenAddr := fmt.Sprintf("localhost:%d", listenPort)
	clusterAddr := fmt.Sprintf("localhost:%d", clusterPort)

	// Create temporary directory for this node
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("ocache-cluster-test-node-%d-*", nodeIndex))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Build seed list
	var seeds []string
	h.mu.RLock()
	numExistingNodes := len(h.Nodes)
	h.mu.RUnlock()

	if numExistingNodes > 0 {
		for i := 0; i < h.NodeCount; i++ {
			seedClusterPort := h.BasePort + 1000 + i
			seedAddr := fmt.Sprintf("localhost:%d", seedClusterPort)
			if seedAddr != clusterAddr {
				seeds = append(seeds, seedAddr)
			}
		}
	}

	// Initialize isolated storage instance for this node
	storageConfig := &storage.StorageConfig{
		DiskPath:           tmpDir,
		TTL:                0,
		InlineThreshold:    int(h.Config.InlineThreshold),
		CompactThreshold:   h.Config.CompactThreshold,
		SegmentSize:        h.Config.SegmentSize,
		FdCacheSize:        h.Config.FDCacheSize,
		MaxDiskUsage:       h.Config.MaxDiskUsage,
		CompactionInterval: h.Config.CompactionInterval,
		CompactionThreads:  h.Config.CompactionThreads,
		MinSegmentAge:      h.Config.RecompactMinSegmentAge,
		MinSegments:        h.Config.RecompactMinSegments,
		CleanupInterval:    h.Config.CleanupInterval,
		AccessUpdateDelay:  h.Config.AccessUpdateDelay,
	}
	stor, err := storage.NewStorageWithConfig(storageConfig)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Create coordinator config
	coordConfig := &coordinator.Config{
		Enabled:            true,
		MyNodeID:           nodeID,
		ClusterAddr:        clusterAddr,
		ListenAddr:         listenAddr,
		Nodes:              seeds,
		RingPartitionCount: hash.DefaultPartitionCount,
		HeartbeatInterval:  1, // 1 second for faster testing
		FailureThreshold:   3,
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
		cancel()
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Create gRPC server with cache service
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
	)

	// Register cache service with node-specific storage instance
	cacheService := newTestCacheService(coord, stor)
	pb.RegisterCacheServiceServer(grpcServer, cacheService)

	// Start gRPC server
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		coord.Stop()
		cancel()
		stor.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	// Channel to signal when server is ready
	serverStartedCh := make(chan struct{})

	go func() {
		close(serverStartedCh)
		if err := grpcServer.Serve(listener); err != nil {
			h.T.Logf("gRPC server stopped for node %s: %v", nodeID, err)
		}
	}()

	// Wait for server to start
	<-serverStartedCh
	time.Sleep(500 * time.Millisecond)

	// Create cluster client connection for topology queries
	clusterConn, err := grpc.Dial(clusterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
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
		ctx:             ctx,
		cancel:          cancel,
		listener:        listener,
		serverStartedCh: serverStartedCh,
	}

	h.mu.Lock()
	h.Nodes[nodeID] = node
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

// IsConverged checks if all nodes have the same view
func (h *ClusterTestHarness) IsConverged() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.Nodes) == 0 {
		return false
	}

	var referenceTopology *clusterpb.ClusterTopology
	var referenceNodeID string

	// Get reference topology from first running node
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

		referenceTopology = topology
		referenceNodeID = nodeID
		break
	}

	if referenceTopology == nil {
		return false
	}

	// Check expected node count
	expectedNodes := 0
	for _, node := range h.Nodes {
		if node.IsRunning() {
			expectedNodes++
		}
	}

	if len(referenceTopology.Nodes) != expectedNodes {
		return false
	}

	// Compare with other nodes
	for nodeID, node := range h.Nodes {
		if !node.IsRunning() || nodeID == referenceNodeID {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		topology, err := node.ClusterSvc.GetClusterTopology(ctx, &clusterpb.Empty{})
		cancel()

		if err != nil {
			return false
		}

		if topology.Epoch != referenceTopology.Epoch || len(topology.Nodes) != len(referenceTopology.Nodes) {
			return false
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

// GetStorageStats returns aggregate storage statistics across all nodes
func (h *ClusterTestHarness) GetStorageStats() StorageStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := StorageStats{}

	for _, node := range h.Nodes {
		if !node.IsRunning() || node.Storage == nil {
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
	h.Metrics.EndTime = time.Now()
	close(h.stopMetrics)
	h.StopAllNodes()
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

// newTestCacheService creates a cache service for testing
func newTestCacheService(coord *coordinator.Coordinator, stor *storage.Storage) pb.CacheServiceServer {
	return &testCacheService{
		coordinator: coord,
		storage:     stor,
	}
}

// testCacheService is a minimal cache service implementation for testing
type testCacheService struct {
	pb.UnimplementedCacheServiceServer
	coordinator *coordinator.Coordinator
	storage     *storage.Storage
}

// Put implements streaming Put for the cache service
func (s *testCacheService) Put(stream pb.CacheService_PutServer) error {
	// Read the first chunk to get key and ttl
	firstChunk, err := stream.Recv()
	if err != nil {
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}

	key := firstChunk.Key
	ttl := int(firstChunk.TtlSeconds)

	if key == "" {
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: "missing key"})
	}

	// Collect all data from stream
	var buf bytes.Buffer
	if len(firstChunk.Data) > 0 {
		buf.Write(firstChunk.Data)
	}

	// Read remaining chunks
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		buf.Write(chunk.Data)
	}

	// Store in node-specific storage instance
	if s.storage == nil {
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: "storage not initialized"})
	}

	err = s.storage.Put(key, bytes.NewReader(buf.Bytes()), ttl)
	if err != nil {
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}

	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// Get implements streaming Get for the cache service
func (s *testCacheService) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	if s.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	reader, exists, err := s.storage.Get(req.Key, req.Start, req.End)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("key not found: %s", req.Key)
	}

	// Close reader if it's a ReadCloser
	if rc, ok := reader.(io.ReadCloser); ok {
		defer rc.Close()
	}

	// Stream data back
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.GetResponse{
				Data: buf[:n],
			}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete implements Delete for the cache service
func (s *testCacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	err := s.storage.DeleteKey(req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// List implements streaming List for the cache service
func (s *testCacheService) List(req *pb.ListRequest, stream pb.CacheService_ListServer) error {
	if s.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	keys, err := s.storage.ListKeys(req.Prefix)
	if err != nil {
		return err
	}

	// Stream keys back in batches
	for _, key := range keys {
		if err := stream.Send(&pb.ListResponse{Keys: []string{key}}); err != nil {
			return err
		}
	}

	return nil
}
