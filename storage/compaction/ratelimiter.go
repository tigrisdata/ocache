// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

const compactionCopyChunkSize = 64 * 1024

// NewCompactionRateLimiter creates the shared payload budget used by all
// background compaction work. A positive limit allows one startup burst no
// larger than 64 KiB or the configured per-second rate, whichever is smaller.
// A non-positive limit leaves a directly constructed Compactor unthrottled.
func NewCompactionRateLimiter(bytesPerSecond int64) *rate.Limiter {
	if bytesPerSecond <= 0 {
		return nil
	}

	burst := compactionCopyChunkSize
	if bytesPerSecond < int64(burst) {
		burst = int(bytesPerSecond)
	}
	return rate.NewLimiter(rate.Limit(bytesPerSecond), burst)
}

// rateLimitedReader reserves source-read capacity before it asks the backing
// file for a payload chunk. Limiting admission before Read keeps background
// compaction from issuing an unbounded read burst on the shared volume. The
// expected length also prevents a reader from supplying bytes beyond the
// ValueMessage that will be published.
type rateLimitedReader struct {
	ctx       context.Context
	reader    io.Reader
	remaining int64
	limiter   *rate.Limiter
}

func newRateLimitedReader(ctx context.Context, reader io.Reader, length int64, limiter *rate.Limiter) *rateLimitedReader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &rateLimitedReader{
		ctx:       ctx,
		reader:    reader,
		remaining: length,
		limiter:   limiter,
	}
}

func (r *rateLimitedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	chunk := len(p)
	if chunk > compactionCopyChunkSize {
		chunk = compactionCopyChunkSize
	}
	if int64(chunk) > r.remaining {
		chunk = int(r.remaining)
	}
	if r.limiter != nil {
		if burst := r.limiter.Burst(); burst > 0 && chunk > burst {
			chunk = burst
		}
	}

	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.limiter != nil {
		if err := r.limiter.WaitN(r.ctx, chunk); err != nil {
			return 0, err
		}
	}

	n, err := r.reader.Read(p[:chunk])
	r.remaining -= int64(n)
	return n, err
}
