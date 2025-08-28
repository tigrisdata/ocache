package errors

import (
	"context"
	"math"
	"math/rand"
	"time"

	zlog "github.com/rs/zerolog/log"
)

// RetryConfig holds configuration for retry logic
type RetryConfig struct {
	MaxAttempts       int           // Maximum number of attempts (including first try)
	InitialBackoff    time.Duration // Initial backoff duration
	MaxBackoff        time.Duration // Maximum backoff duration
	BackoffMultiplier float64       // Multiplier for exponential backoff
	JitterFraction    float64       // Fraction of backoff to use as jitter (0-1)
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.1,
	}
}

// FastRetryConfig returns configuration for fast retries (e.g., file locks)
func FastRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        500 * time.Millisecond,
		BackoffMultiplier: 1.5,
		JitterFraction:    0.2,
	}
}

// RetryWithBackoff executes a function with exponential backoff retry for transient errors
func RetryWithBackoff(ctx context.Context, config *RetryConfig, operation string, fn func() error) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	var lastErr error
	backoff := config.InitialBackoff

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Execute the function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retriable
		if !IsRetriable(err) {
			return err
		}

		// Check if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			zlog.Warn().
				Str("operation", operation).
				Int("attempts", attempt).
				Err(err).
				Msg("storage: max retry attempts reached")
			return err
		}

		// Check context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Calculate sleep duration with jitter
		sleepDuration := backoff
		if config.JitterFraction > 0 {
			jitter := time.Duration(rand.Float64() * config.JitterFraction * float64(backoff))
			sleepDuration = backoff + jitter
		}

		zlog.Debug().
			Str("operation", operation).
			Int("attempt", attempt).
			Dur("backoff", sleepDuration).
			Err(err).
			Msg("storage: retrying after transient error")

		// Sleep with context cancellation support
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return ctx.Err()
		}

		// Calculate next backoff
		backoff = time.Duration(float64(backoff) * config.BackoffMultiplier)
		if backoff > config.MaxBackoff {
			backoff = config.MaxBackoff
		}
	}

	return lastErr
}

// RetryOperation is a simpler retry helper for operations that don't need context
func RetryOperation(operation string, fn func() error) error {
	return RetryWithBackoff(context.Background(), DefaultRetryConfig(), operation, fn)
}

// RetryOperationFast uses faster retry intervals for operations like file locks
func RetryOperationFast(operation string, fn func() error) error {
	return RetryWithBackoff(context.Background(), FastRetryConfig(), operation, fn)
}

// calculateBackoff calculates the next backoff duration
func calculateBackoff(current time.Duration, multiplier float64, max time.Duration) time.Duration {
	next := time.Duration(math.Min(float64(current)*multiplier, float64(max)))
	return next
}
