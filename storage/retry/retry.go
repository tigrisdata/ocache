// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"math/rand"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/storage/errors"
)

// Config holds retry configuration
type Config struct {
	InitialDelay time.Duration // Initial delay before first retry
	MaxDelay     time.Duration // Maximum delay between retries
	MaxRetries   int           // Maximum number of retry attempts
	Multiplier   float64       // Backoff multiplier
	JitterFactor float64       // Jitter factor (0.0 to 1.0)
}

// DefaultConfig returns the default retry configuration
func DefaultConfig() Config {
	return Config{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		MaxRetries:   5,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}
}

// FastConfig returns a configuration for fast retries (used for lock conflicts)
func FastConfig() Config {
	return Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		MaxRetries:   10,
		Multiplier:   1.5,
		JitterFactor: 0.2,
	}
}

// Operation represents a retryable operation
type Operation func() error

// Do executes an operation with exponential backoff retry for retryable errors
func Do(ctx context.Context, cfg Config, op string, fn Operation) error {
	return DoWithKey(ctx, cfg, op, "", fn)
}

// DoWithKey executes an operation with exponential backoff retry for retryable errors,
// including key information for better logging
func DoWithKey(ctx context.Context, cfg Config, op string, key string, fn Operation) error {
	var lastErr error
	delay := cfg.InitialDelay

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return errors.NewTimeoutError(op, key)
		default:
		}

		// Execute the operation
		err := fn()
		if err == nil {
			if attempt > 0 {
				zlog.Debug().
					Str("op", op).
					Str("key", key).
					Int("attempts", attempt+1).
					Msg("retry: operation succeeded")
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !errors.IsRetryable(err) {
			zlog.Debug().
				Str("op", op).
				Str("key", key).
				Err(err).
				Msg("retry: non-retryable error")
			return err
		}

		// Don't retry if we've exhausted attempts
		if attempt == cfg.MaxRetries {
			zlog.Warn().
				Str("op", op).
				Str("key", key).
				Int("attempts", attempt+1).
				Err(err).
				Msg("retry: max retries exhausted")
			break
		}

		// Calculate next delay with exponential backoff and jitter
		nextDelay := calculateDelay(delay, cfg.MaxDelay, cfg.Multiplier, cfg.JitterFactor)

		zlog.Debug().
			Str("op", op).
			Str("key", key).
			Int("attempt", attempt+1).
			Dur("delay", nextDelay).
			Err(err).
			Msg("retry: retryable error, backing off")

		// Wait with context cancellation support
		timer := time.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.NewTimeoutError(op, key)
		case <-timer.C:
		}

		delay = nextDelay
	}

	// Return the last error after all retries are exhausted
	return lastErr
}

// calculateDelay calculates the next delay with exponential backoff and jitter
func calculateDelay(currentDelay, maxDelay time.Duration, multiplier, jitterFactor float64) time.Duration {
	// Apply exponential backoff
	nextDelay := time.Duration(float64(currentDelay) * multiplier)

	// Cap at max delay
	if nextDelay > maxDelay {
		nextDelay = maxDelay
	}

	// Add jitter to prevent thundering herd
	if jitterFactor > 0 {
		jitter := time.Duration(rand.Float64() * jitterFactor * float64(nextDelay))
		if rand.Intn(2) == 0 {
			nextDelay += jitter
		} else {
			nextDelay -= jitter
		}

		// Ensure delay doesn't go negative
		if nextDelay < 0 {
			nextDelay = time.Millisecond
		}
	}

	return nextDelay
}

// RetryableReader wraps an io.Reader to add retry capability for read operations
type RetryableReader struct {
	ctx    context.Context
	cfg    Config
	op     string
	key    string
	reader func() ([]byte, error)
}

// NewRetryableReader creates a new retryable reader
func NewRetryableReader(ctx context.Context, cfg Config, op, key string, reader func() ([]byte, error)) *RetryableReader {
	return &RetryableReader{
		ctx:    ctx,
		cfg:    cfg,
		op:     op,
		key:    key,
		reader: reader,
	}
}

// Read implements io.Reader with retry logic
func (r *RetryableReader) Read(p []byte) (n int, err error) {
	var data []byte
	err = DoWithKey(r.ctx, r.cfg, r.op, r.key, func() error {
		var readErr error
		data, readErr = r.reader()
		return readErr
	})

	if err != nil {
		return 0, err
	}

	n = copy(p, data)
	return n, nil
}

// Backoff provides a simple backoff iterator for custom retry loops
type Backoff struct {
	cfg     Config
	attempt int
	delay   time.Duration
}

// NewBackoff creates a new backoff iterator
func NewBackoff(cfg Config) *Backoff {
	return &Backoff{
		cfg:   cfg,
		delay: cfg.InitialDelay,
	}
}

// Next returns the next backoff delay and whether to continue retrying
func (b *Backoff) Next() (time.Duration, bool) {
	if b.attempt >= b.cfg.MaxRetries {
		return 0, false
	}

	delay := b.delay
	b.delay = calculateDelay(b.delay, b.cfg.MaxDelay, b.cfg.Multiplier, b.cfg.JitterFactor)
	b.attempt++

	return delay, true
}

// Reset resets the backoff to initial state
func (b *Backoff) Reset() {
	b.attempt = 0
	b.delay = b.cfg.InitialDelay
}

// Attempts returns the number of attempts made
func (b *Backoff) Attempts() int {
	return b.attempt
}
