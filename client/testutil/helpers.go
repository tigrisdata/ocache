package testutil

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertEventuallyConsistent checks that a condition becomes true within a timeout
func AssertEventuallyConsistent(t *testing.T, condition func() bool, timeout time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Eventually(t, condition, timeout, tick, msgAndArgs...)
}

// RequireEventuallyConsistent requires that a condition becomes true within a timeout
func RequireEventuallyConsistent(t *testing.T, condition func() bool, timeout time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, condition, timeout, tick, msgAndArgs...)
}

// WaitForCondition waits for a condition to become true or returns an error
func WaitForCondition(ctx context.Context, condition func() bool, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}

// RetryWithBackoff retries an operation with exponential backoff
func RetryWithBackoff(ctx context.Context, maxRetries int, initialDelay time.Duration, maxDelay time.Duration, operation func() error) error {
	delay := initialDelay

	for i := 0; i < maxRetries; i++ {
		err := operation()
		if err == nil {
			return nil
		}

		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Exponential backoff with jitter
				delay = delay * 2
				if delay > maxDelay {
					delay = maxDelay
				}
			}
		} else {
			return err // Last attempt failed
		}
	}
	panic("unreachable") // Loop always exits via return
}

// AssertNoError asserts that no error occurred and fails the test if there was
func AssertNoError(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	assert.NoError(t, err, msgAndArgs...)
}

// RequireNoError requires that no error occurred and fails the test if there was
func RequireNoError(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	require.NoError(t, err, msgAndArgs...)
}

// AssertWithinDuration asserts that two times are within a duration of each other
func AssertWithinDuration(t *testing.T, expected, actual time.Time, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	assert.WithinDuration(t, expected, actual, delta, msgAndArgs...)
}

// RunConcurrently runs a function concurrently n times and waits for completion
func RunConcurrently(n int, fn func(id int)) {
	done := make(chan struct{}, n)

	for i := 0; i < n; i++ {
		go func(id int) {
			fn(id)
			done <- struct{}{}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < n; i++ {
		<-done
	}
}

// CollectErrors collects errors from concurrent operations
type ErrorCollector struct {
	errors    []error
	ch        chan error
	collected bool
}

// NewErrorCollector creates a new error collector
func NewErrorCollector(bufferSize int) *ErrorCollector {
	return &ErrorCollector{
		errors: make([]error, 0),
		ch:     make(chan error, bufferSize),
	}
}

// Add adds an error to the collector
func (ec *ErrorCollector) Add(err error) {
	if err != nil && !ec.collected {
		select {
		case ec.ch <- err:
		default:
			// Buffer full, drop error
		}
	}
}

// Collect collects all errors and returns them.
// This method can only be called once - subsequent calls will return the same slice.
func (ec *ErrorCollector) Collect() []error {
	if !ec.collected {
		ec.collected = true
		close(ec.ch)
		for err := range ec.ch {
			ec.errors = append(ec.errors, err)
		}
	}
	return ec.errors
}

// Count returns the number of errors collected
func (ec *ErrorCollector) Count() int {
	return len(ec.Collect())
}

// FailingReader fails after reading a certain amount
type FailingReader struct {
	Data      []byte
	Pos       int
	FailAfter int
}

func (f *FailingReader) Read(p []byte) (n int, err error) {
	if f.Pos >= f.FailAfter {
		return 0, errors.New("simulated read failure")
	}
	remaining := f.FailAfter - f.Pos
	if remaining > len(p) {
		remaining = len(p)
	}
	if remaining > len(f.Data)-f.Pos {
		remaining = len(f.Data) - f.Pos
	}
	if remaining == 0 {
		return 0, io.EOF
	}
	copy(p[:remaining], f.Data[f.Pos:f.Pos+remaining])
	f.Pos += remaining
	return remaining, nil
}

// SafeBuffer is a thread-safe buffer for testing
type SafeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (s *SafeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *SafeBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}

// SafeReader is a thread-safe reader for testing
type SafeReader struct {
	mu   sync.Mutex
	Data []byte
	pos  int
}

func (s *SafeReader) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.Data) {
		return 0, io.EOF
	}
	n = copy(p, s.Data[s.pos:])
	s.pos += n
	return n, nil
}
