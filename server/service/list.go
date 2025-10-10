package service

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxListLimit is the maximum number of keys to return in a single List call
	MaxListLimit = 1000
	// DefaultListLimit is the default limit if not specified
	DefaultListLimit = 1000
)

// ContinuationToken represents the state for resuming a paginated list operation
type ContinuationToken struct {
	NodeCursors map[string]string `json:"node_cursors"` // nodeID -> last_key_from_node
	Prefix      string            `json:"prefix"`       // Original prefix filter
}

// heapNode represents a key from a specific node for the K-way merge
type heapNode struct {
	key    string
	nodeID string
	done   bool // True if this node has no more keys
}

// keyHeap implements heap.Interface for K-way merge of sorted keys
type keyHeap []*heapNode

func (h keyHeap) Len() int           { return len(h) }
func (h keyHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h keyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *keyHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapNode))
}

func (h *keyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// List implements the public List RPC
// In cluster mode, coordinates K-way merge across all nodes
// Returns a single response with sorted, paginated keys and continuation token
func (s *CacheService) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("List").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Str("continuation_token", req.ContinuationToken).
		Msg("gRPC List called")

	// If clustering is enabled, perform K-way merge
	if s.coordinator != nil {
		return s.handleClusteredList(ctx, req)
	}

	// Single-node mode: query local storage directly
	return s.handleLocalList(ctx, req)
}

// ListLocal implements the internal node-local List RPC
// This is used by the coordinator to query individual nodes in cluster mode
// Returns sorted, paginated keys from this node's local storage
func (s *CacheService) ListLocal(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ListLocal").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Msg("gRPC ListLocal called")

	response, err := s.handleLocalList(ctx, req)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("ListLocal", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "ListLocal").Inc()
		return nil, err
	}

	metrics.RPCRequests.WithLabelValues("ListLocal", "success").Inc()

	return response, nil
}

// handleLocalList handles List request on local node only (single-node mode)
func (s *CacheService) handleLocalList(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	// Apply limit
	limit := int(req.Limit)
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// Determine start key: use continuation token if provided, otherwise use start_key
	startKey := req.StartKey
	if req.ContinuationToken != "" {
		startKey = req.ContinuationToken
	}

	// Query local storage with pagination
	var keys []string
	var lastKey string
	var hasMore bool

	err := retry.Do(ctx, retry.DefaultConfig(), "ListKeysWithPagination", func() error {
		var listErr error
		keys, lastKey, hasMore, listErr = s.storage.ListKeysWithPagination(req.Prefix, startKey, limit)
		return listErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "List").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}

	// Build response
	response := &pb.ListResponse{
		Keys:    keys,
		HasMore: hasMore,
	}

	// Add continuation token if there are more results
	if hasMore && lastKey != "" {
		response.ContinuationToken = lastKey
	}

	metrics.RPCRequests.WithLabelValues("List", "success").Inc()
	return response, nil
}

// NodeResponse holds the response from a single node
type NodeResponse struct {
	NodeID            string
	Keys              []string
	ContinuationToken string
	HasMore           bool
}

// handleClusteredList performs K-way merge of responses from all nodes
func (s *CacheService) handleClusteredList(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	// Get all active nodes from the ring
	ring := s.coordinator.GetRing()
	nodes := ring.GetActiveNodes()

	if len(nodes) == 0 {
		metrics.RPCRequests.WithLabelValues("List", "no_nodes").Inc()
		return nil, status.Error(codes.Unavailable, "no active nodes in cluster")
	}

	// Apply limit
	limit := int(req.Limit)
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// Parse continuation token if provided
	nodeCursors, err := s.parseContinuationToken(req.ContinuationToken, req.Prefix)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "invalid_token").Inc()
		return nil, status.Errorf(codes.InvalidArgument, "invalid continuation token: %v", err)
	}

	zlog.Debug().
		Str("prefix", req.Prefix).
		Int("node_count", len(nodes)).
		Int("limit", limit).
		Interface("node_cursors", nodeCursors).
		Msg("Starting cluster-wide List with K-way merge")

	// Fetch keys from all nodes
	nodeResponses, err := s.fetchFromAllNodes(ctx, nodes, req, nodeCursors)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "fetch_error").Inc()
		return nil, err
	}

	if len(nodeResponses) == 0 {
		metrics.RPCRequests.WithLabelValues("List", "no_responses").Inc()
		return &pb.ListResponse{
			Keys:    []string{},
			HasMore: false,
		}, nil
	}

	// Perform K-way merge
	result, newCursors, hasMore, err := s.kWayMerge(ctx, nodeResponses, limit)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "merge_error").Inc()
		return nil, err
	}

	// Build continuation token if needed
	var continuationToken string
	if hasMore {
		continuationToken, err = s.encodeContinuationToken(newCursors, req.Prefix)
		if err != nil {
			zlog.Warn().Err(err).Msg("Failed to encode continuation token")
			// Continue without token rather than failing the request
		}
	}

	response := &pb.ListResponse{
		Keys:              result,
		ContinuationToken: continuationToken,
		HasMore:           hasMore,
	}

	metrics.RPCRequests.WithLabelValues("List", "success").Inc()

	return response, nil
}

// fetchFromAllNodes fetches keys from all nodes in parallel
func (s *CacheService) fetchFromAllNodes(ctx context.Context, nodes []*coordinator.NodeInfo, req *pb.ListRequest, nodeCursors map[string]string) (map[string]*NodeResponse, error) {
	localNodeID := s.coordinator.GetLocalNodeID()
	router := s.coordinator.GetRouter()

	responses := make(map[string]*NodeResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *coordinator.NodeInfo) {
			defer wg.Done()

			// Determine start key for this node
			startKey := req.StartKey
			if cursor, exists := nodeCursors[n.ID]; exists && cursor != "" {
				startKey = cursor
			}

			// Build request for this node
			nodeReq := &pb.ListRequest{
				Prefix:   req.Prefix,
				StartKey: startKey,
				Limit:    req.Limit, // Each node gets full limit; we'll merge and truncate
			}

			var resp *pb.ListResponse
			var err error

			// If this is the local node, call directly
			if n.ID == localNodeID {
				resp, err = s.ListLocal(ctx, nodeReq)
			} else {
				// Remote node: get client
				client, clientErr := router.GetClientForNode(n.ID)
				if clientErr != nil {
					zlog.Warn().Err(clientErr).Str("node_id", n.ID).Msg("Failed to get client for node, skipping")
					return
				}

				resp, err = client.ListLocal(ctx, nodeReq)
			}

			if err != nil {
				zlog.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to list from node, skipping")
				return
			}

			// Store response
			mu.Lock()
			responses[n.ID] = &NodeResponse{
				NodeID:            n.ID,
				Keys:              resp.Keys,
				ContinuationToken: resp.ContinuationToken,
				HasMore:           resp.HasMore,
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	zlog.Debug().Int("response_count", len(responses)).Int("node_count", len(nodes)).Msg("Fetched keys from nodes")
	return responses, nil
}

// kWayMerge performs K-way merge of sorted responses from nodes
func (s *CacheService) kWayMerge(ctx context.Context, nodeResponses map[string]*NodeResponse, limit int) ([]string, map[string]string, bool, error) {
	// Initialize min-heap
	h := &keyHeap{}
	heap.Init(h)

	// Track node cursors (last key from each node)
	nodeCursors := make(map[string]string)

	// Track key indices for each node
	nodeIndices := make(map[string]int)

	// Prime the heap with first key from each node
	for nodeID, nodeResp := range nodeResponses {
		if len(nodeResp.Keys) > 0 {
			heap.Push(h, &heapNode{
				key:    nodeResp.Keys[0],
				nodeID: nodeID,
				done:   false,
			})
			nodeIndices[nodeID] = 0
		} else {
			// Initialize index even for empty responses to track HasMore state
			nodeIndices[nodeID] = -1
		}
	}

	// Collect merged results
	result := make([]string, 0, limit)
	seenKeys := make(map[string]bool) // Deduplication (defensive)

	// Merge until we have enough keys or heap is empty
	for h.Len() > 0 && len(result) < limit {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		default:
		}

		// Pop minimum key
		minNode := heap.Pop(h).(*heapNode)

		// Always update cursor for this node (even if duplicate)
		nodeCursors[minNode.nodeID] = minNode.key

		// Deduplicate (shouldn't happen with proper partitioning, but handle it)
		addedToResult := false
		if !seenKeys[minNode.key] {
			result = append(result, minNode.key)
			seenKeys[minNode.key] = true
			addedToResult = true
		}

		// Try to get next key from this node's response
		nodeResp := nodeResponses[minNode.nodeID]
		currentIndex := nodeIndices[minNode.nodeID]
		nextIndex := currentIndex + 1

		if nextIndex < len(nodeResp.Keys) {
			// More keys available in this node's response
			heap.Push(h, &heapNode{
				key:    nodeResp.Keys[nextIndex],
				nodeID: minNode.nodeID,
				done:   false,
			})
			nodeIndices[minNode.nodeID] = nextIndex
		} else if nodeResp.HasMore {
			// Node has more keys but we've exhausted this response
			// The continuation token is already in nodeResp.ContinuationToken
			// We'll use it for the next page request
		}

		// If we hit limit due to a duplicate key, continue to fill up to limit
		// This ensures we always return 'limit' unique keys when available
		if !addedToResult && len(result) >= limit {
			// We skipped a duplicate but already have enough results
			break
		}
	}

	// Determine if there are more keys available
	// hasMore is true if:
	// 1. We still have keys in the heap (hit limit), OR
	// 2. Any node has HasMore=true and we've consumed all its keys
	hasMore := h.Len() > 0

	if !hasMore {
		// Check if any node has more data
		for nodeID, nodeResp := range nodeResponses {
			if nodeResp.HasMore {
				// Check if we've consumed all keys from this node
				// nodeIndices[nodeID] could be -1 (empty response) or the last index
				idx, exists := nodeIndices[nodeID]
				if !exists || idx >= len(nodeResp.Keys)-1 {
					hasMore = true
					// Update cursor to the continuation token from the node
					if nodeResp.ContinuationToken != "" {
						nodeCursors[nodeID] = nodeResp.ContinuationToken
					}
				}
			}
		}
	}

	return result, nodeCursors, hasMore, nil
}

// parseContinuationToken decodes the continuation token
func (s *CacheService) parseContinuationToken(token string, prefix string) (map[string]string, error) {
	if token == "" {
		return make(map[string]string), nil
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid base64: %w", err)
	}

	// Parse JSON
	var ct ContinuationToken
	if err := json.Unmarshal(decoded, &ct); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Validate prefix matches
	if ct.Prefix != prefix {
		return nil, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", ct.Prefix, prefix)
	}

	return ct.NodeCursors, nil
}

// encodeContinuationToken creates a continuation token from node cursors
func (s *CacheService) encodeContinuationToken(nodeCursors map[string]string, prefix string) (string, error) {
	if len(nodeCursors) == 0 {
		return "", nil
	}

	ct := ContinuationToken{
		NodeCursors: nodeCursors,
		Prefix:      prefix,
	}

	// Encode as JSON
	jsonBytes, err := json.Marshal(ct)
	if err != nil {
		return "", err
	}

	// Encode as base64
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}
