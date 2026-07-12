// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorageError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *StorageError
		expected string
	}{
		{
			name:     "with key",
			err:      NewNotFoundError("Get", "testkey"),
			expected: "Get testkey: key not found",
		},
		{
			name:     "without key",
			err:      NewInvalidRequestError("Put", "missing key"),
			expected: "Put: missing key",
		},
		{
			name:     "storage full",
			err:      NewStorageFullError("Put", nil),
			expected: "Put: storage capacity exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestStorageError_Retryability(t *testing.T) {
	tests := []struct {
		name      string
		err       *StorageError
		retryable bool
	}{
		{
			name:      "not found - non-retryable",
			err:       NewNotFoundError("Get", "key"),
			retryable: false,
		},
		{
			name:      "invalid request - non-retryable",
			err:       NewInvalidRequestError("Put", "bad request"),
			retryable: false,
		},
		{
			name:      "storage full - non-retryable",
			err:       NewStorageFullError("Put", nil),
			retryable: false,
		},
		{
			name:      "corruption - non-retryable",
			err:       NewCorruptionError("Get", "key", nil),
			retryable: false,
		},
		{
			name:      "temporary - retryable",
			err:       NewTemporaryError("Get", "key", nil),
			retryable: true,
		},
		{
			name:      "IO read - retryable",
			err:       NewIORetryableError("Get", "key", nil),
			retryable: true,
		},
		{
			name:      "IO write - non-retryable",
			err:       NewIOError("Put", "key", nil),
			retryable: false,
		},
		{
			name:      "lock - retryable",
			err:       NewLockError("Get", "key", nil),
			retryable: true,
		},
		{
			name:      "timeout - retryable",
			err:       NewTimeoutError("Get", "key"),
			retryable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.retryable, tt.err.IsRetryable())
			assert.Equal(t, tt.retryable, IsRetryable(tt.err))
		})
	}
}

func TestStorageError_TypeChecking(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		isNotFound    bool
		isRetryable   bool
		isStorageFull bool
		isCorruption  bool
		expectedType  ErrorType
	}{
		{
			name:         "not found",
			err:          NewNotFoundError("Get", "key"),
			isNotFound:   true,
			isRetryable:  false,
			expectedType: TypeNotFound,
		},
		{
			name:          "storage full",
			err:           NewStorageFullError("Put", nil),
			isStorageFull: true,
			isRetryable:   false,
			expectedType:  TypeStorageFull,
		},
		{
			name:         "corruption",
			err:          NewCorruptionError("Get", "key", nil),
			isCorruption: true,
			isRetryable:  false,
			expectedType: TypeCorruption,
		},
		{
			name:         "temporary",
			err:          NewTemporaryError("Get", "key", nil),
			isRetryable:  true,
			expectedType: TypeTemporary,
		},
		{
			name:         "non-storage error",
			err:          errors.New("random error"),
			isNotFound:   false,
			isRetryable:  false,
			expectedType: TypeInternal, // Default for non-storage errors
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isNotFound, IsNotFound(tt.err))
			assert.Equal(t, tt.isRetryable, IsRetryable(tt.err))
			assert.Equal(t, tt.isStorageFull, IsStorageFull(tt.err))
			assert.Equal(t, tt.isCorruption, IsCorruption(tt.err))

			errType, ok := GetType(tt.err)
			if _, isStorageErr := tt.err.(*StorageError); isStorageErr {
				assert.True(t, ok)
				assert.Equal(t, tt.expectedType, errType)
			} else {
				assert.False(t, ok)
				assert.Equal(t, TypeInternal, errType)
			}
		})
	}
}

func TestStorageError_Unwrap(t *testing.T) {
	underlying := errors.New("underlying error")
	err := NewIORetryableError("Get", "key", underlying)

	assert.Equal(t, underlying, err.Unwrap())
	assert.True(t, errors.Is(err, underlying))
}
