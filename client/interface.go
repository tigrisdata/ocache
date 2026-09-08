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

	// Conditional (compare-and-swap) operations (issue #254). A write applies
	// only if the key's current version equals the caller's expectation; a lost
	// race returns a *VersionMismatchError carrying the current version. An
	// absent key reads with an observation token rather than 0 (issue #267):
	// passing it back as expected orders the write against any CAS delete
	// stamped after the observation, while expected == 0 remains the unordered
	// put-if-absent. A CAS delete of a missing or dead key records that fence.
	// Note: adding these methods extends CacheClient — external implementers
	// must add them too.
	GetWithVersion(ctx context.Context, key string) (data []byte, version uint64, found bool, err error)
	PutIfVersion(ctx context.Context, key string, data []byte, ttlSeconds int64, expected uint64) (newVersion uint64, err error)
	DeleteIfVersion(ctx context.Context, key string, expected uint64) error

	// Streaming CAS for values larger than the unary message cap (issue #258).
	// Same semantics as PutIfVersion/GetWithVersion; the value is streamed
	// rather than buffered.
	PutStreamIfVersion(ctx context.Context, key string, r io.Reader, ttlSeconds int64, expected uint64) (newVersion uint64, err error)
	GetStreamWithVersion(ctx context.Context, key string, w io.Writer) (version uint64, found bool, err error)
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
