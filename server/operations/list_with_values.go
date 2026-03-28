package operations

import (
	"context"

	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/retry"
)

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
		merged, token, hasMore, err := o.listClusterWide(ctx, prefix, limit, continuationToken, true)
		done(err)
		if err != nil {
			return nil, "", false, err
		}
		return merged.Entries, token, hasMore, nil
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
