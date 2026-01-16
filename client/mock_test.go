package cacheclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// hashKeyToToken hashes a key to a token using FNV-1a 32-bit (same as server)
func hashKeyToToken(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

const bufSize = 1024 * 1024 // 1MB buffer for in-memory connection

// mockCacheServiceServer implements pb.CacheServiceServer for testing
type mockCacheServiceServer struct {
	pb.UnimplementedCacheServiceServer

	// Storage for testing
	data     map[string][]byte
	dataMu   sync.RWMutex
	ttls     map[string]int64
	metadata map[string]map[string]string

	// Control behavior
	putError       error
	getError       error
	deleteError    error
	listError      error
	streamErrors   map[string]error // key -> error for streaming
	partialData    map[string]int   // key -> bytes to send before error
	operationDelay map[string]int   // operation -> delay in ms

	// Tracking
	putCallCount         atomic.Int32
	getCallCount         atomic.Int32
	deleteCallCount      atomic.Int32
	listCallCount        atomic.Int32
	getTopologyCallCount atomic.Int32

	// Node ownership simulation (for cluster mode)
	nodeID      string
	ownedTokens []uint32 // Sorted list of tokens owned by this node

	// Cluster topology for GetTopology
	clusterTopology   *clusterpb.ClusterTopology
	clusterTopologyMu sync.RWMutex
}

func newMockCacheServiceServer() *mockCacheServiceServer {
	return &mockCacheServiceServer{
		data:           make(map[string][]byte),
		ttls:           make(map[string]int64),
		metadata:       make(map[string]map[string]string),
		streamErrors:   make(map[string]error),
		partialData:    make(map[string]int),
		operationDelay: make(map[string]int),
		ownedTokens:    nil,
	}
}

// ownsKey checks if this node owns the key based on token ring
func (m *mockCacheServiceServer) ownsKey(key string) bool {
	if m.nodeID == "" || len(m.ownedTokens) == 0 {
		return true // No ownership configured, accept all
	}

	keyToken := hashKeyToToken(key)

	// Binary search for the first token >= keyToken
	idx := sort.Search(len(m.ownedTokens), func(i int) bool {
		return m.ownedTokens[i] >= keyToken
	})

	// Wrap around if past the last token
	if idx == len(m.ownedTokens) {
		idx = 0
	}

	// Check if this token is in our owned set
	// Note: In a real ring, we'd check if the owning node matches,
	// but for tests we just check if it's in our token range
	return true // For simplicity, we'll use the full token check below
}

func (m *mockCacheServiceServer) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	m.putCallCount.Add(1)

	if m.putError != nil {
		return nil, m.putError
	}

	// Check if this node owns the key (cluster mode simulation)
	// Note: For simplicity in tests, we accept all keys when ownedTokens is configured
	// The real routing happens in the client via TokenRing

	m.dataMu.Lock()
	defer m.dataMu.Unlock()

	m.data[req.Key] = req.Data
	if req.TtlSeconds > 0 {
		m.ttls[req.Key] = req.TtlSeconds
	}

	return &pb.PutResponse{Success: true}, nil
}

func (m *mockCacheServiceServer) Put(stream pb.CacheService_PutServer) error {
	m.putCallCount.Add(1)

	if m.putError != nil {
		return m.putError
	}

	var key string
	var ttl int64
	var data []byte
	first := true

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if first {
			key = req.Key
			ttl = req.TtlSeconds
			first = false
		}
		data = append(data, req.Data...)
	}

	// Check ownership - in tests, we accept all keys since routing happens in client
	m.dataMu.Lock()
	m.data[key] = data
	if ttl > 0 {
		m.ttls[key] = ttl
	}
	m.dataMu.Unlock()

	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

func (m *mockCacheServiceServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	m.getCallCount.Add(1)

	if m.getError != nil {
		return m.getError
	}

	// Check ownership - in tests, we accept all keys since routing happens in client
	m.dataMu.RLock()
	data, exists := m.data[req.Key]
	m.dataMu.RUnlock()

	if !exists {
		return status.Error(codes.NotFound, "key not found")
	}

	// Handle range requests (inclusive end semantics; end <= 0 means read to EOF)
	start := int64(0)
	end := int64(len(data)) - 1 // inclusive: last valid index
	if req.Start > 0 {
		start = req.Start
	}
	if req.End > 0 {
		end = req.End
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
	}
	if start >= int64(len(data)) || start > end {
		return status.Error(codes.InvalidArgument, "invalid range")
	}
	data = data[start : end+1] // +1 because Go slices are exclusive

	// Handle simulated partial data errors
	if partialBytes, hasPartial := m.partialData[req.Key]; hasPartial {
		if partialBytes > 0 && partialBytes < len(data) {
			// Send partial data then error
			if err := stream.Send(&pb.GetResponse{Data: data[:partialBytes]}); err != nil {
				return err
			}
			if streamErr, hasErr := m.streamErrors[req.Key]; hasErr {
				return streamErr
			}
			return status.Error(codes.Unavailable, "simulated stream error")
		}
	}

	// Handle simulated stream errors without partial data
	if streamErr, hasErr := m.streamErrors[req.Key]; hasErr {
		return streamErr
	}

	// Send data in chunks
	chunkSize := 64 * 1024 // 64KB chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.GetResponse{Data: data[i:end]}); err != nil {
			return err
		}
	}

	return nil
}

func (m *mockCacheServiceServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	m.deleteCallCount.Add(1)

	if m.deleteError != nil {
		return nil, m.deleteError
	}

	// Check ownership - in tests, we accept all keys since routing happens in client
	m.dataMu.Lock()
	defer m.dataMu.Unlock()

	if _, exists := m.data[req.Key]; !exists {
		return nil, status.Error(codes.NotFound, "key not found")
	}

	delete(m.data, req.Key)
	delete(m.ttls, req.Key)
	delete(m.metadata, req.Key)

	return &pb.DeleteResponse{Success: true}, nil
}

func (m *mockCacheServiceServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	m.listCallCount.Add(1)

	if m.listError != nil {
		return nil, m.listError
	}

	m.dataMu.RLock()
	defer m.dataMu.RUnlock()

	var keys []string
	for key := range m.data {
		if req.Prefix == "" || len(key) >= len(req.Prefix) && key[:len(req.Prefix)] == req.Prefix {
			keys = append(keys, key)
		}
	}

	// Sort keys to match real behavior
	sort.Strings(keys)

	// Apply pagination
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000
	}
	if limit > 1000 {
		limit = 1000
	}

	startIdx := 0
	foundStart := req.StartKey == ""
	if req.StartKey != "" {
		// Find first key after startKey
		for i, k := range keys {
			if k > req.StartKey {
				startIdx = i
				foundStart = true
				break
			}
		}
		// If startKey is greater than all existing keys, return empty
		if !foundStart {
			startIdx = len(keys)
		}
	}

	endIdx := startIdx + limit
	hasMore := endIdx < len(keys)
	if endIdx > len(keys) {
		endIdx = len(keys)
	}

	resultKeys := keys[startIdx:endIdx]
	var continuationToken string
	if hasMore && len(resultKeys) > 0 {
		continuationToken = resultKeys[len(resultKeys)-1]
	}

	return &pb.ListResponse{
		Keys:              resultKeys,
		ContinuationToken: continuationToken,
		HasMore:           hasMore,
	}, nil
}

// ListLocal is identical to List for the mock
func (m *mockCacheServiceServer) ListLocal(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return m.List(ctx, req)
}

// SetClusterTopology safely sets the cluster topology
func (m *mockCacheServiceServer) SetClusterTopology(topology *clusterpb.ClusterTopology) {
	m.clusterTopologyMu.Lock()
	defer m.clusterTopologyMu.Unlock()
	m.clusterTopology = topology
}

// GetTopology returns the cluster topology (converting from ClusterService format)
func (m *mockCacheServiceServer) GetTopology(ctx context.Context, req *pb.GetTopologyRequest) (*pb.GetTopologyResponse, error) {
	m.getTopologyCallCount.Add(1)

	m.clusterTopologyMu.RLock()
	defer m.clusterTopologyMu.RUnlock()

	// For single-node tests without cluster setup
	if m.clusterTopology == nil {
		return &pb.GetTopologyResponse{
			Error: "cluster mode not enabled",
		}, nil
	}

	// Make a deep copy to avoid race conditions
	topologyCopy := proto.Clone(m.clusterTopology).(*clusterpb.ClusterTopology)

	// Return the topology copy
	return &pb.GetTopologyResponse{
		Topology: topologyCopy,
	}, nil
}

// mockClusterServiceServer implements clusterpb.ClusterServiceServer for testing
type mockClusterServiceServer struct {
	clusterpb.UnimplementedClusterServiceServer

	topology      *clusterpb.ClusterTopology
	topologyError error
	mu            sync.RWMutex

	getTopologyCallCount atomic.Int32
}

func newMockClusterServiceServer() *mockClusterServiceServer {
	return &mockClusterServiceServer{}
}

func (m *mockClusterServiceServer) GetClusterTopology(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterTopology, error) {
	m.getTopologyCallCount.Add(1)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.topologyError != nil {
		return nil, m.topologyError
	}

	if m.topology == nil {
		return nil, status.Error(codes.NotFound, "topology not available")
	}

	return m.topology, nil
}

func (m *mockClusterServiceServer) SetTopology(topology *clusterpb.ClusterTopology) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topology = topology
}

func (m *mockClusterServiceServer) SetTopologyError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topologyError = err
}

// testServer manages a mock gRPC server for testing
type testServer struct {
	listener     *bufconn.Listener
	grpcServer   *grpc.Server
	cacheService *mockCacheServiceServer
	address      string
}

// newTestServer creates a new in-memory test server
func newTestServer() *testServer {
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()

	cacheService := newMockCacheServiceServer()

	pb.RegisterCacheServiceServer(grpcServer, cacheService)

	ts := &testServer{
		listener:     listener,
		grpcServer:   grpcServer,
		cacheService: cacheService,
		address:      "bufnet",
	}

	// Start server in background
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			// Server stopped
		}
	}()

	return ts
}

// newTestServerWithAddr creates a test server listening on a real address
func newTestServerWithAddr() (*testServer, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer()
	cacheService := newMockCacheServiceServer()

	pb.RegisterCacheServiceServer(grpcServer, cacheService)

	ts := &testServer{
		listener:     nil,
		grpcServer:   grpcServer,
		cacheService: cacheService,
		address:      listener.Addr().String(),
	}

	// Start server in background
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			// Server stopped
		}
	}()

	return ts, nil
}

func (ts *testServer) Stop() {
	ts.grpcServer.Stop()
}

func (ts *testServer) Dialer() func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		if ts.listener == nil {
			return nil, fmt.Errorf("dialer not available for real address servers")
		}
		return ts.listener.Dial()
	}
}

// Helper functions for common test scenarios

// setupSimpleTopology creates a basic topology for testing with token assignments
func setupSimpleTopology(nodes []string) *clusterpb.ClusterTopology {
	// Use 128 tokens per node for testing (same as dskit default for tests)
	tokensPerNode := 128

	topology := &clusterpb.ClusterTopology{
		Epoch: 1,
		Nodes: make([]*clusterpb.NodeInfo, 0, len(nodes)),
		RingConfig: &clusterpb.RingConfig{
			ReplicationFactor: 1, // Data replication (1 = no replication)
			NodeTokens:        make([]*clusterpb.NodeTokens, 0, len(nodes)),
		},
	}

	// Generate tokens for each node
	// Distribute tokens evenly across the uint32 space
	tokenRange := uint32(0xFFFFFFFF) / uint32(len(nodes)*tokensPerNode)

	for i, node := range nodes {
		nodeID := fmt.Sprintf("node-%d", i)

		nodeInfo := &clusterpb.NodeInfo{
			Id:            nodeID,
			Address:       node,
			ListenAddress: node, // For tests, use the same address for both cluster and listen
			Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			JoinedAt:      uint64(i),
			Weight:        1.0,
		}
		topology.Nodes = append(topology.Nodes, nodeInfo)

		// Generate sorted tokens for this node
		tokens := make([]uint32, tokensPerNode)
		baseToken := uint32(i) * tokenRange * uint32(tokensPerNode)
		for t := 0; t < tokensPerNode; t++ {
			tokens[t] = baseToken + uint32(t)*tokenRange
		}
		sort.Slice(tokens, func(a, b int) bool { return tokens[a] < tokens[b] })

		topology.RingConfig.NodeTokens = append(topology.RingConfig.NodeTokens, &clusterpb.NodeTokens{
			NodeId: nodeID,
			Tokens: tokens,
		})
	}

	return topology
}

// setupMultiNodeTestServers creates multiple test servers simulating a cluster
func setupMultiNodeTestServers(count int) ([]*testServer, *clusterpb.ClusterTopology, error) {
	servers := make([]*testServer, count)
	addresses := make([]string, count)

	// Create servers
	for i := 0; i < count; i++ {
		server, err := newTestServerWithAddr()
		if err != nil {
			// Clean up already created servers
			for j := 0; j < i; j++ {
				servers[j].Stop()
			}
			return nil, nil, err
		}
		servers[i] = server
		addresses[i] = server.address
	}

	// Create topology with token assignments
	topology := setupSimpleTopology(addresses)

	// Configure each server with topology and ownership info
	for i, server := range servers {
		// Set topology in cache service for GetTopology
		server.cacheService.SetClusterTopology(topology)

		nodeID := fmt.Sprintf("node-%d", i)
		server.cacheService.nodeID = nodeID

		// Set owned tokens from topology
		for _, nt := range topology.RingConfig.NodeTokens {
			if nt.NodeId == nodeID {
				server.cacheService.ownedTokens = make([]uint32, len(nt.Tokens))
				copy(server.cacheService.ownedTokens, nt.Tokens)
				break
			}
		}
	}

	return servers, topology, nil
}

// configurableResponse allows setting custom responses for testing
type configurableResponse struct {
	putResponse    *pb.PutResponse
	putError       error
	getData        []byte
	getError       error
	deleteResponse *pb.DeleteResponse
	deleteError    error
	listKeys       []string
	listError      error
}

func (ts *testServer) ConfigureResponse(config *configurableResponse) {
	if config.putError != nil {
		ts.cacheService.putError = config.putError
	}
	if config.getError != nil {
		ts.cacheService.getError = config.getError
	}
	if config.getData != nil {
		ts.cacheService.data["test-key"] = config.getData
	}
	if config.deleteError != nil {
		ts.cacheService.deleteError = config.deleteError
	}
	if config.listError != nil {
		ts.cacheService.listError = config.listError
	}
	if config.listKeys != nil {
		for _, key := range config.listKeys {
			ts.cacheService.data[key] = []byte("test-data")
		}
	}
}

// errorInjector simulates various error conditions
type errorInjector struct {
	routingError     bool
	networkError     bool
	timeoutError     bool
	partialDataBytes int
	errorAfterBytes  int
}

func (ts *testServer) InjectErrors(key string, injector *errorInjector) {
	if injector.routingError {
		ts.cacheService.streamErrors[key] = status.Error(codes.FailedPrecondition, "routing error")
	}
	if injector.networkError {
		ts.cacheService.streamErrors[key] = status.Error(codes.Unavailable, "network error")
	}
	if injector.timeoutError {
		ts.cacheService.streamErrors[key] = status.Error(codes.DeadlineExceeded, "timeout")
	}
	if injector.partialDataBytes > 0 {
		ts.cacheService.partialData[key] = injector.partialDataBytes
	}
}

// Helper to reset all errors and data
func (ts *testServer) Reset() {
	ts.cacheService.putError = nil
	ts.cacheService.getError = nil
	ts.cacheService.deleteError = nil
	ts.cacheService.listError = nil
	ts.cacheService.streamErrors = make(map[string]error)
	ts.cacheService.partialData = make(map[string]int)
	ts.cacheService.data = make(map[string][]byte)
	ts.cacheService.ttls = make(map[string]int64)
	ts.cacheService.metadata = make(map[string]map[string]string)
	ts.cacheService.putCallCount.Store(0)
	ts.cacheService.getCallCount.Store(0)
	ts.cacheService.deleteCallCount.Store(0)
	ts.cacheService.listCallCount.Store(0)
}

// GetCallCounts returns the call counts for assertions
func (ts *testServer) GetCallCounts() (put, get, delete, list int32) {
	return ts.cacheService.putCallCount.Load(),
		ts.cacheService.getCallCount.Load(),
		ts.cacheService.deleteCallCount.Load(),
		ts.cacheService.listCallCount.Load()
}
