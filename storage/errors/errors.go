package errors

import (
	"errors"
	"fmt"
)

// ErrorType represents the category of storage error
type ErrorType int

const (
	// TypeNotFound indicates the requested resource was not found
	TypeNotFound ErrorType = iota
	// TypeInvalidRequest indicates the request parameters were invalid
	TypeInvalidRequest
	// TypeStorageFull indicates storage capacity has been exceeded
	TypeStorageFull
	// TypeCorruption indicates data corruption was detected
	TypeCorruption
	// TypeTemporary indicates a transient error that may succeed on retry
	TypeTemporary
	// TypeIO indicates a file system I/O error
	TypeIO
	// TypeLock indicates a resource locking conflict
	TypeLock
	// TypeTimeout indicates an operation timed out
	TypeTimeout
	// TypeInternal indicates an unexpected internal error
	TypeInternal
)

// StorageError represents a high-level storage error that abstracts
// internal implementation details
type StorageError struct {
	Type       ErrorType
	Op         string // Operation that failed (e.g., "Get", "Put", "Delete")
	Key        string // Key involved in the operation, if applicable
	Message    string // User-friendly error message
	Retryable  bool   // Whether the operation can be retried
	underlying error  // Internal error for debugging (not exposed)
}

// Error implements the error interface
func (e *StorageError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s %s: %s", e.Op, e.Key, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Message)
}

// Unwrap returns the underlying error for internal debugging
func (e *StorageError) Unwrap() error {
	return e.underlying
}

// IsRetryable returns whether this error represents a retryable condition
func (e *StorageError) IsRetryable() bool {
	return e.Retryable
}

// GetType returns the error type
func (e *StorageError) GetType() ErrorType {
	return e.Type
}

// Common error constructors

// NewNotFoundError creates a new not-found error
func NewNotFoundError(op, key string) *StorageError {
	return &StorageError{
		Type:      TypeNotFound,
		Op:        op,
		Key:       key,
		Message:   "key not found",
		Retryable: false,
	}
}

// NewInvalidRequestError creates a new invalid request error
func NewInvalidRequestError(op, message string) *StorageError {
	return &StorageError{
		Type:      TypeInvalidRequest,
		Op:        op,
		Message:   message,
		Retryable: false,
	}
}

// NewStorageFullError creates a new storage full error
func NewStorageFullError(op string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeStorageFull,
		Op:         op,
		Message:    "storage capacity exceeded",
		Retryable:  false,
		underlying: underlying,
	}
}

// NewCorruptionError creates a new corruption error
func NewCorruptionError(op, key string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeCorruption,
		Op:         op,
		Key:        key,
		Message:    "data corruption detected",
		Retryable:  false,
		underlying: underlying,
	}
}

// NewTemporaryError creates a new temporary error that can be retried
func NewTemporaryError(op, key string, underlying error) *StorageError {
	message := "temporary storage error"
	// You can add more specific message handling here if needed
	return &StorageError{
		Type:       TypeTemporary,
		Op:         op,
		Key:        key,
		Message:    message,
		Retryable:  true,
		underlying: underlying,
	}
}

// NewIOError creates a new I/O error
func NewIOError(op, key string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeIO,
		Op:         op,
		Key:        key,
		Message:    "storage I/O error",
		Retryable:  false,
		underlying: underlying,
	}
}

// NewIORetryableError creates a new I/O error that is retryable
func NewIORetryableError(op, key string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeIO,
		Op:         op,
		Key:        key,
		Message:    "storage I/O error",
		Retryable:  true,
		underlying: underlying,
	}
}

// NewLockError creates a new lock conflict error
func NewLockError(op, key string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeLock,
		Op:         op,
		Key:        key,
		Message:    "resource is temporarily locked",
		Retryable:  true,
		underlying: underlying,
	}
}

// NewTimeoutError creates a new timeout error
func NewTimeoutError(op, key string) *StorageError {
	return &StorageError{
		Type:      TypeTimeout,
		Op:        op,
		Key:       key,
		Message:   "operation timed out",
		Retryable: true,
	}
}

// NewInternalError creates a new internal error for unexpected conditions
func NewInternalError(op string, underlying error) *StorageError {
	return &StorageError{
		Type:       TypeInternal,
		Op:         op,
		Message:    "internal storage error",
		Retryable:  false,
		underlying: underlying,
	}
}

// Helper functions for error checking

// IsNotFound checks if an error is a not-found error
func IsNotFound(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Type == TypeNotFound
}

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Retryable
}

// IsStorageFull checks if an error indicates storage is full
func IsStorageFull(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Type == TypeStorageFull
}

// IsCorruption checks if an error indicates data corruption
func IsCorruption(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Type == TypeCorruption
}

// GetType extracts the error type from an error
func GetType(err error) (ErrorType, bool) {
	var storageErr *StorageError
	if errors.As(err, &storageErr) {
		return storageErr.Type, true
	}
	return TypeInternal, false
}

// IsTimeout checks if an error is a timeout error
func IsTimeout(err error) bool {
	var storageErr *StorageError
	return errors.As(err, &storageErr) && storageErr.Type == TypeTimeout
}
