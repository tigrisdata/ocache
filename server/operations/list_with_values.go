package operations

import (
	"context"
	"fmt"
	"sync"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/retry"
)

// NodeKVResponse holds the response from a single node for key-value listing.
type NodeKVResponse struct {
	NodeID            string
	Entries           []*pb.KeyValue
	ContinuationToken string
	HasMore           bool
}

// ListPageWithValues returns a page of key-value pairs with pagination support.
// In cluster mode, performs K-way merge from all nodes.
// In single-node mode, queries local storage directly.
func (o *Operations) ListPageWithValues(ctx context.Context, prefix string, limit int, continuationToken string) ([]*pb.KeyValue, string, bool, error) {
	done := recordOperationStart("Operations.ListPageWithValues")

	zlog.Debug().
		Str("prefix", prefix).
		Int("limit", limit).
		Str("continuation_token", continuationToken).
		Msg("Operations.ListPageWithValues called")

	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	if o.IsClusterMode() {
		entries, token, hasMore, err := o.listClusterWideWithValues(ctx, prefix, limit, continuationToken)
		done(err)
		return entries, token, hasMore, err
	}

	entries, token, hasMore, err := o.ListLocalWithValues(ctx, prefix, limit, continuationToken)
	done(err)
	return entries, token, hasMore, err
}

// ListLocalWithValues returns key-value pairs from local storage only.
func (o *Operations) ListLocalWithValues(ctx context.Context, prefix string, limit int, continuationToken string) ([]*pb.KeyValue, string, bool, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	startKey := continuationToken

	var storageEntries []stor.KeyValue
	var lastKey string
	var hasMore bool

	err := retry.Do(ctx, retry.DefaultConfig(), "ListKeyValuesWithPagination", func() error {
		var listErr error
		storageEntries, lastKey, hasMore, listErr = o.storage.ListKeyValuesWithPagination(prefix, startKey, limit)
		return listErr
	})
	if err != nil {
		return nil, "", false, err
	}

	// Convert storage entries to proto entries
	entries := make([]*pb.KeyValue, len(storageEntries))
	for i, e := range storageEntries {
		entries[i] = &pb.KeyValue{
			Key:   e.Key,
			Value: e.Value,
		}
	}

	var nextToken string
	if hasMore && lastKey != "" {
		nextToken = lastKey
	}

	return entries, nextToken, hasMore, nil
}

// listClusterWideWithValues performs K-way merge of key-value responses from all nodes.
func (o *Operations) listClusterWideWithValues(ctx context.Context, prefix string, limit int, continuationToken string) ([]*pb.KeyValue, string, bool, error) {
	ringMgr := o.GetRing()
	nodes := ringMgr.GetActiveNodes()

	if len(nodes) == 0 {
		return nil, "", false, fmt.Errorf("no active nodes in cluster")
	}

	nodeCursors, isPlainStartKey, err := o.parseContinuationToken(continuationToken, prefix)
	if err != nil {
		return nil, "", false, fmt.Errorf("invalid continuation token: %w", err)
	}

	if isPlainStartKey && continuationToken != "" {
		nodeCursors = make(map[string]string)
		for _, node := range nodes {
			nodeCursors[node.ID] = continuationToken
		}
	}

	zlog.Debug().
		Str("prefix", prefix).
		Int("node_count", len(nodes)).
		Int("limit", limit).
		Interface("node_cursors", nodeCursors).
		Msg("Starting cluster-wide ListWithValues with K-way merge")

	nodeResponses, err := o.fetchKVFromAllNodes(ctx, nodes, prefix, limit, nodeCursors)
	if err != nil {
		return nil, "", false, err
	}

	if len(nodeResponses) == 0 {
		return []*pb.KeyValue{}, "", false, nil
	}

	result, newCursors, hasMore, err := o.kWayMergeKV(ctx, nodeResponses, limit)
	if err != nil {
		return nil, "", false, err
	}

	var newToken string
	if hasMore {
		newToken, err = o.encodeContinuationToken(newCursors, prefix)
		if err != nil {
			zlog.Warn().Err(err).Msg("Failed to encode continuation token")
		}
	}

	return result, newToken, hasMore, nil
}

// fetchKVFromAllNodes fetches key-value pairs from all nodes in parallel.
func (o *Operations) fetchKVFromAllNodes(ctx context.Context, nodes []*ring.NodeInfo, prefix string, limit int, nodeCursors map[string]string) (map[string]*NodeKVResponse, error) {
	localNodeID := o.GetLocalNodeID()
	router := o.GetRouter()

	responses := make(map[string]*NodeKVResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *ring.NodeInfo) {
			defer wg.Done()

			startKey := ""
			if cursor, exists := nodeCursors[n.ID]; exists && cursor != "" {
				startKey = cursor
			}

			var entries []*pb.KeyValue
			var nextToken string
			var hasMore bool
			var err error

			if n.ID == localNodeID {
				entries, nextToken, hasMore, err = o.ListLocalWithValues(ctx, prefix, limit, startKey)
			} else {
				client, clientErr := router.GetClientForNode(n.ID)
				if clientErr != nil {
					zlog.Warn().Err(clientErr).Str("node_id", n.ID).Msg("Failed to get client for node, skipping")
					return
				}

				req := &pb.ListRequest{
					Prefix:   prefix,
					StartKey: startKey,
					Limit:    int32(limit),
				}

				resp, respErr := client.ListLocalWithValues(ctx, req)
				if respErr != nil {
					err = respErr
				} else {
					entries = resp.Entries
					nextToken = resp.ContinuationToken
					hasMore = resp.HasMore
				}
			}

			if err != nil {
				zlog.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to list with values from node, skipping")
				return
			}

			mu.Lock()
			responses[n.ID] = &NodeKVResponse{
				NodeID:            n.ID,
				Entries:           entries,
				ContinuationToken: nextToken,
				HasMore:           hasMore,
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	zlog.Debug().Int("response_count", len(responses)).Int("node_count", len(nodes)).Msg("Fetched KV entries from nodes")
	return responses, nil
}

// kWayMergeKV performs K-way merge of sorted key-value responses from nodes.
func (o *Operations) kWayMergeKV(ctx context.Context, nodeResponses map[string]*NodeKVResponse, limit int) ([]*pb.KeyValue, map[string]string, bool, error) {
	// Simple slice-based min selection (same logic as kWayMerge but with KV entries)
	nodeCursors := make(map[string]string)
	nodeIndices := make(map[string]int)

	// Build initial candidates
	type candidate struct {
		entry  *pb.KeyValue
		nodeID string
	}
	candidates := make([]candidate, 0, len(nodeResponses))

	for nodeID, nodeResp := range nodeResponses {
		if len(nodeResp.Entries) > 0 {
			candidates = append(candidates, candidate{entry: nodeResp.Entries[0], nodeID: nodeID})
			nodeIndices[nodeID] = 0
		} else {
			nodeIndices[nodeID] = -1
		}
	}

	result := make([]*pb.KeyValue, 0, limit)
	seenKeys := make(map[string]bool)

	for len(candidates) > 0 && len(result) < limit {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		default:
		}

		// Find minimum key among candidates
		minIdx := 0
		for i := 1; i < len(candidates); i++ {
			if candidates[i].entry.Key < candidates[minIdx].entry.Key {
				minIdx = i
			}
		}

		min := candidates[minIdx]
		nodeCursors[min.nodeID] = min.entry.Key

		if !seenKeys[min.entry.Key] {
			result = append(result, min.entry)
			seenKeys[min.entry.Key] = true
		}

		// Advance this node's index
		nodeResp := nodeResponses[min.nodeID]
		nextIdx := nodeIndices[min.nodeID] + 1
		nodeIndices[min.nodeID] = nextIdx

		if nextIdx < len(nodeResp.Entries) {
			candidates[minIdx] = candidate{entry: nodeResp.Entries[nextIdx], nodeID: min.nodeID}
		} else {
			// Remove exhausted node from candidates
			candidates[minIdx] = candidates[len(candidates)-1]
			candidates = candidates[:len(candidates)-1]
		}
	}

	hasMore := len(candidates) > 0
	if !hasMore {
		for nodeID, nodeResp := range nodeResponses {
			if nodeResp.HasMore {
				idx := nodeIndices[nodeID]
				if idx >= len(nodeResp.Entries)-1 {
					hasMore = true
					if nodeResp.ContinuationToken != "" {
						nodeCursors[nodeID] = nodeResp.ContinuationToken
					}
				}
			}
		}
	}

	return result, nodeCursors, hasMore, nil
}
