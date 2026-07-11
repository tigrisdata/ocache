// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"io"
)

// KeyValue holds a key and its associated value bytes.
type KeyValue struct {
	Key   string
	Value []byte
	// ValueLength is the size of the value in bytes, set even when the value was
	// omitted for exceeding the List-with-values per-value size cap.
	ValueLength int64
	// ValueOmitted is true when the value was omitted for exceeding that cap;
	// Value is nil in that case.
	ValueOmitted bool
}

// CacheClient is the common interface for both SimpleClient and ClusterClient
type CacheClient interface {
	// Basic operations
	Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	ListPage(ctx context.Context, prefix string, limit int, continuationToken string) (keys []string, nextToken string, hasMore bool, err error)
	ListPageWithValues(ctx context.Context, prefix string, limit int, continuationToken string) (entries []KeyValue, nextToken string, hasMore bool, err error)

	// Streaming operations
	PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error
	GetStream(ctx context.Context, key string, w io.Writer) error

	// Range operations
	GetRange(ctx context.Context, key string, start, end int64) ([]byte, error)
	GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error

	// Lifecycle
	Close() error

	// Info
	GetMode() ConnectionMode
	GetConnectedNodes() []string
}
