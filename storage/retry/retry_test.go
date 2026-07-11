// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
)

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	cfg := Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   3,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}

	callCount := 0
	err := Do(context.Background(), cfg, "test", func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestDo_RetryOnRetryableError(t *testing.T) {
	cfg := Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   3,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}

	callCount := 0
	err := Do(context.Background(), cfg, "test", func() error {
		callCount++
		if callCount < 3 {
			return storageErrors.NewTemporaryError("test", "key", nil)
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount)
}

func TestDo_NoRetryOnNonRetryableError(t *testing.T) {
	cfg := Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   3,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}

	callCount := 0
	err := Do(context.Background(), cfg, "test", func() error {
		callCount++
		return storageErrors.NewNotFoundError("test", "key")
	})

	assert.Error(t, err)
	assert.True(t, storageErrors.IsNotFound(err))
	assert.Equal(t, 1, callCount) // Should not retry
}

func TestDo_MaxRetriesExhausted(t *testing.T) {
	cfg := Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   2,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}

	callCount := 0
	err := Do(context.Background(), cfg, "test", func() error {
		callCount++
		return storageErrors.NewTemporaryError("test", "key", nil)
	})

	assert.Error(t, err)
	assert.True(t, storageErrors.IsRetryable(err))
	assert.Equal(t, 3, callCount) // Initial + 2 retries
}

func TestDo_ContextCancellation(t *testing.T) {
	cfg := Config{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		MaxRetries:   5,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	// Cancel context after first attempt
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, cfg, "test", func() error {
		callCount++
		return storageErrors.NewTemporaryError("test", "key", nil)
	})

	assert.Error(t, err)
	assert.True(t, storageErrors.IsTimeout(err))
	assert.Equal(t, 1, callCount) // Should stop after context cancellation
}

func TestBackoff_Iterator(t *testing.T) {
	cfg := Config{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   3,
		Multiplier:   2.0,
		JitterFactor: 0.0, // No jitter for predictable testing
	}

	backoff := NewBackoff(cfg)

	// First retry
	delay1, ok := backoff.Next()
	assert.True(t, ok)
	assert.Equal(t, 10*time.Millisecond, delay1)
	assert.Equal(t, 1, backoff.Attempts())

	// Second retry
	delay2, ok := backoff.Next()
	assert.True(t, ok)
	assert.Equal(t, 20*time.Millisecond, delay2)
	assert.Equal(t, 2, backoff.Attempts())

	// Third retry
	delay3, ok := backoff.Next()
	assert.True(t, ok)
	assert.Equal(t, 40*time.Millisecond, delay3)
	assert.Equal(t, 3, backoff.Attempts())

	// Should fail - max retries exhausted
	_, ok = backoff.Next()
	assert.False(t, ok)
	assert.Equal(t, 3, backoff.Attempts())

	// Reset
	backoff.Reset()
	assert.Equal(t, 0, backoff.Attempts())

	// Should work again after reset
	delay4, ok := backoff.Next()
	assert.True(t, ok)
	assert.Equal(t, 10*time.Millisecond, delay4)
	assert.Equal(t, 1, backoff.Attempts())
}

func TestCalculateDelay_MaxDelayCap(t *testing.T) {
	// Test that delay is capped at max delay
	delay := calculateDelay(100*time.Millisecond, 50*time.Millisecond, 2.0, 0.0)
	assert.Equal(t, 50*time.Millisecond, delay)
}

func TestCalculateDelay_WithJitter(t *testing.T) {
	// With jitter, the delay should vary
	delays := make(map[time.Duration]bool)
	for i := 0; i < 10; i++ {
		delay := calculateDelay(100*time.Millisecond, 1*time.Second, 2.0, 0.5)
		delays[delay] = true
	}

	// Should have some variation
	assert.Greater(t, len(delays), 1)

	// All delays should be within reasonable bounds
	for delay := range delays {
		assert.Greater(t, delay, time.Duration(0))
		assert.LessOrEqual(t, delay, 300*time.Millisecond) // 200ms + 50% jitter
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, 100*time.Millisecond, cfg.InitialDelay)
	assert.Equal(t, 10*time.Second, cfg.MaxDelay)
	assert.Equal(t, 5, cfg.MaxRetries)
	assert.Equal(t, 2.0, cfg.Multiplier)
	assert.Equal(t, 0.1, cfg.JitterFactor)
}

func TestFastConfig(t *testing.T) {
	cfg := FastConfig()
	assert.Equal(t, 10*time.Millisecond, cfg.InitialDelay)
	assert.Equal(t, 1*time.Second, cfg.MaxDelay)
	assert.Equal(t, 10, cfg.MaxRetries)
	assert.Equal(t, 1.5, cfg.Multiplier)
	assert.Equal(t, 0.2, cfg.JitterFactor)
}
