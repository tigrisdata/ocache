// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Router error types
var (
	// ErrNodeNotFound indicates the target node doesn't exist in the ring
	ErrNodeNotFound = errors.New("node not found in ring")

	// ErrCircuitBreakerOpen indicates the circuit breaker is open for a node
	ErrCircuitBreakerOpen = errors.New("circuit breaker open")

	// ErrLocalRouting indicates an attempt to route to the local node
	ErrLocalRouting = errors.New("cannot route to local node")

	// ErrNoAvailableNode indicates no node is available for the key
	ErrNoAvailableNode = errors.New("no available node for key")

	// ErrConnectionFailed indicates failure to establish connection
	ErrConnectionFailed = errors.New("failed to establish connection")

	// ErrMaxRetriesExceeded indicates all retry attempts failed
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// RouterError represents a routing error with additional context
type RouterError struct {
	Type    error  // The base error type
	NodeID  string // The node that caused the error
	Key     string // The key being routed (if applicable)
	Message string // Additional context
	Cause   error  // The underlying error (if any)
}

// Error implements the error interface
func (e *RouterError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%v: %s (node=%s, key=%s): %v", e.Type, e.Message, e.NodeID, e.Key, e.Cause)
	}
	if e.Message != "" {
		return fmt.Sprintf("%v: %s (node=%s, key=%s)", e.Type, e.Message, e.NodeID, e.Key)
	}
	return fmt.Sprintf("%v (node=%s, key=%s)", e.Type, e.NodeID, e.Key)
}

// Unwrap returns the underlying error type for errors.Is
func (e *RouterError) Unwrap() error {
	return e.Type
}

// Is implements error matching for RouterError
func (e *RouterError) Is(target error) bool {
	if e.Type != nil {
		return errors.Is(e.Type, target)
	}
	return false
}

// NewNodeNotFoundError creates a new node not found error
func NewNodeNotFoundError(nodeID string, key string) *RouterError {
	return &RouterError{
		Type:    ErrNodeNotFound,
		NodeID:  nodeID,
		Key:     key,
		Message: "node not found in ring",
	}
}

// NewCircuitBreakerOpenError creates a new circuit breaker open error
func NewCircuitBreakerOpenError(nodeID string) *RouterError {
	return &RouterError{
		Type:    ErrCircuitBreakerOpen,
		NodeID:  nodeID,
		Message: "circuit breaker is open",
	}
}

// NewLocalRoutingError creates a new local routing error
func NewLocalRoutingError(nodeID string, key string) *RouterError {
	return &RouterError{
		Type:    ErrLocalRouting,
		NodeID:  nodeID,
		Key:     key,
		Message: "caller should check IsLocal first",
	}
}

// NewConnectionFailedError creates a new connection failed error
func NewConnectionFailedError(nodeID string, address string, cause error) *RouterError {
	return &RouterError{
		Type:    ErrConnectionFailed,
		NodeID:  nodeID,
		Message: fmt.Sprintf("failed to connect to %s", address),
		Cause:   cause,
	}
}

// NewMaxRetriesExceededError creates a new max retries exceeded error
func NewMaxRetriesExceededError(nodeID string, key string, attempts int, cause error) *RouterError {
	return &RouterError{
		Type:    ErrMaxRetriesExceeded,
		NodeID:  nodeID,
		Key:     key,
		Message: fmt.Sprintf("failed after %d attempts", attempts),
		Cause:   cause,
	}
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific non-retryable errors
	var routerErr *RouterError
	if errors.As(err, &routerErr) {
		switch {
		case errors.Is(routerErr, ErrCircuitBreakerOpen):
			return false // Don't retry when circuit breaker is open
		case errors.Is(routerErr, ErrLocalRouting):
			return false // Don't retry local routing errors
		case errors.Is(routerErr, ErrNodeNotFound):
			return false // Don't retry if node doesn't exist
		case errors.Is(routerErr, ErrMaxRetriesExceeded):
			return false // Already retried max times
		case errors.Is(routerErr, ErrConnectionFailed):
			return true // Retry connection failures
		}
	}

	// Check if it's a routing error (backward compatibility)
	return IsRoutingError(err)
}

// IsTemporaryError checks if an error is temporary and might succeed on retry
func IsTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	var routerErr *RouterError
	if errors.As(err, &routerErr) {
		return errors.Is(routerErr, ErrConnectionFailed)
	}

	return false
}

// IsRoutingError checks if an error is a transient routing error that should be retried
func IsRoutingError(err error) bool {
	if err == nil {
		return false
	}

	// Check RouterError types directly to avoid infinite recursion
	switch {
	case errors.Is(err, ErrCircuitBreakerOpen):
		return false
	case errors.Is(err, ErrLocalRouting):
		return false
	case errors.Is(err, ErrNodeNotFound):
		return false
	case errors.Is(err, ErrMaxRetriesExceeded):
		return false
	case errors.Is(err, ErrConnectionFailed):
		return true
	}

	// Check gRPC status errors
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted:
		return true
	default:
		return false
	}
}
