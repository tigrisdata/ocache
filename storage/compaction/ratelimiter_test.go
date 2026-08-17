// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/deletion"
	"golang.org/x/time/rate"
)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

func TestNewCompactionRateLimiter(t *testing.T) {
	require.Nil(t, NewCompactionRateLimiter(0))

	t.Run("caps a small startup burst at the configured rate", func(t *testing.T) {
		limiter := NewCompactionRateLimiter(12345)
		require.Equal(t, rate.Limit(12345), limiter.Limit())
		require.Equal(t, 12345, limiter.Burst())
	})
	t.Run("caps a large startup burst at one copy chunk", func(t *testing.T) {
		limiter := NewCompactionRateLimiter(2 * compactionCopyChunkSize)
		require.Equal(t, rate.Limit(2*compactionCopyChunkSize), limiter.Limit())
		require.Equal(t, compactionCopyChunkSize, limiter.Burst())
	})
}

func TestRateLimitedReaderReservesBeforeBackingRead(t *testing.T) {
	read := false
	reader := newRateLimitedReader(context.Background(), readerFunc(func([]byte) (int, error) {
		read = true
		return 0, io.EOF
	}), 1, rate.NewLimiter(1, 0))

	_, err := reader.Read(make([]byte, 1))
	require.Error(t, err)
	require.False(t, read, "the backing reader must not run when admission fails")
}

func TestRateLimitedReaderBoundsSourceReadSize(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 2*compactionCopyChunkSize)
	reader := newRateLimitedReader(context.Background(), bytes.NewReader(payload), int64(len(payload)), rate.NewLimiter(rate.Inf, compactionCopyChunkSize))
	buffer := make([]byte, len(payload))

	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, compactionCopyChunkSize, n)

	n, err = reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, compactionCopyChunkSize, n)

	n, err = reader.Read(buffer)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestRateLimitedReaderRespectsLimiterBurst(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 8)
	reader := newRateLimitedReader(context.Background(), bytes.NewReader(payload), int64(len(payload)), rate.NewLimiter(rate.Inf, 3))

	n, err := reader.Read(make([]byte, len(payload)))
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

func TestNewCompactorSharesRateLimiterWithRecompaction(t *testing.T) {
	_, meta, fileManager, segmentManager, cleanup := setupTestEnvironment(t)
	defer cleanup()

	compactor := NewCompactorWithConfig(&CompactorConfig{
		MetaDB:                   meta,
		FileManager:              fileManager,
		SegmentManager:           segmentManager,
		DeletionQueue:            deletion.NewQueue(meta, defaultDeletionQueueConfig()),
		CompactionBytesPerSecond: 1024,
		EnableRecompaction:       true,
		FragThreshold:            0.5,
		MinSegmentAge:            time.Hour,
		MinSegments:              2,
		RecompactionInterval:     time.Minute,
	})

	require.NotNil(t, compactor.rateLimiter)
	require.NotNil(t, compactor.recompactor)
	require.Same(t, compactor.rateLimiter, compactor.recompactor.rateLimiter)
}
