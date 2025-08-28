package errors

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryWithBackoff(t *testing.T) {
	t.Run("successful on first attempt", func(t *testing.T) {
		attempts := 0
		err := RetryWithBackoff(context.Background(), DefaultRetryConfig(), "test", func() error {
			attempts++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("successful after retry", func(t *testing.T) {
		attempts := 0
		err := RetryWithBackoff(context.Background(), DefaultRetryConfig(), "test", func() error {
			attempts++
			if attempts < 3 {
				return ErrTemporary // Retriable error
			}
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("non-retriable error stops immediately", func(t *testing.T) {
		attempts := 0
		nonRetriableErr := errors.New("permanent error")

		err := RetryWithBackoff(context.Background(), DefaultRetryConfig(), "test", func() error {
			attempts++
			return nonRetriableErr
		})

		assert.Error(t, err)
		assert.Equal(t, nonRetriableErr, err)
		assert.Equal(t, 1, attempts) // Should not retry
	})

	t.Run("max attempts reached", func(t *testing.T) {
		config := &RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        50 * time.Millisecond,
			BackoffMultiplier: 2,
		}

		attempts := 0
		err := RetryWithBackoff(context.Background(), config, "test", func() error {
			attempts++
			return ErrTemporary // Always retriable
		})

		assert.Error(t, err)
		assert.Equal(t, ErrTemporary, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		attempts := int32(0)

		// Cancel context after first attempt
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		err := RetryWithBackoff(ctx, DefaultRetryConfig(), "test", func() error {
			atomic.AddInt32(&attempts, 1)
			return ErrTemporary
		})

		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.LessOrEqual(t, int(atomic.LoadInt32(&attempts)), 2) // Should stop early
	})

	t.Run("exponential backoff timing", func(t *testing.T) {
		config := &RetryConfig{
			MaxAttempts:       4,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
			JitterFraction:    0, // No jitter for predictable testing
		}

		attempts := 0
		start := time.Now()

		err := RetryWithBackoff(context.Background(), config, "test", func() error {
			attempts++
			if attempts < 4 {
				return ErrTemporary
			}
			return nil
		})

		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Equal(t, 4, attempts)

		// Expected total sleep: 10ms + 20ms + 40ms = 70ms
		// Allow some tolerance for execution time
		assert.Greater(t, duration, 60*time.Millisecond)
		assert.Less(t, duration, 100*time.Millisecond)
	})
}

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	assert.Equal(t, 3, config.MaxAttempts)
	assert.Equal(t, 50*time.Millisecond, config.InitialBackoff)
	assert.Equal(t, 2*time.Second, config.MaxBackoff)
	assert.Equal(t, 2.0, config.BackoffMultiplier)
	assert.Equal(t, 0.1, config.JitterFraction)
}

func TestFastRetryConfig(t *testing.T) {
	config := FastRetryConfig()

	assert.Equal(t, 5, config.MaxAttempts)
	assert.Equal(t, 10*time.Millisecond, config.InitialBackoff)
	assert.Equal(t, 500*time.Millisecond, config.MaxBackoff)
	assert.Equal(t, 1.5, config.BackoffMultiplier)
	assert.Equal(t, 0.2, config.JitterFraction)
}

func TestRetryOperation(t *testing.T) {
	attempts := 0
	err := RetryOperation("test", func() error {
		attempts++
		if attempts < 2 {
			return ErrTemporary
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestRetryOperationFast(t *testing.T) {
	attempts := 0
	start := time.Now()

	err := RetryOperationFast("test", func() error {
		attempts++
		if attempts < 3 {
			return ErrLocked
		}
		return nil
	})

	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
	// Should be faster than default retry
	assert.Less(t, duration, 100*time.Millisecond)
}

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name       string
		current    time.Duration
		multiplier float64
		max        time.Duration
		expected   time.Duration
	}{
		{
			name:       "normal multiplication",
			current:    10 * time.Millisecond,
			multiplier: 2.0,
			max:        100 * time.Millisecond,
			expected:   20 * time.Millisecond,
		},
		{
			name:       "capped at max",
			current:    50 * time.Millisecond,
			multiplier: 3.0,
			max:        100 * time.Millisecond,
			expected:   100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateBackoff(tt.current, tt.multiplier, tt.max)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRetryWithJitter(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.5, // 50% jitter
	}

	attempts := 0
	start := time.Now()

	err := RetryWithBackoff(context.Background(), config, "test", func() error {
		attempts++
		if attempts < 3 {
			return ErrTemporary
		}
		return nil
	})

	duration := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)

	// With jitter, timing is less predictable, but should be in a range
	// Base sleep would be 20ms + 40ms = 60ms
	// With 50% jitter, could be up to 90ms
	assert.Greater(t, duration, 40*time.Millisecond)
	assert.Less(t, duration, 120*time.Millisecond)
}
