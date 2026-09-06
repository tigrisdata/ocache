// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"fmt"
)

// VersionMismatchError reports a lost conditional (CAS) operation: the key's
// current version did not equal the caller's expected version (issue #254).
// CurrentVersion carries the version observed after the operation resolved —
// 0 means the key is absent — so the caller can re-read, re-decide, and retry
// with a fresh precondition. It is deliberately not a StorageError: it is not
// retryable as-is (retrying with the same expectation loses again by
// definition) and represents a normal outcome, not a failure.
type VersionMismatchError struct {
	Op             string
	Key            string
	CurrentVersion uint64
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("%s %s: version mismatch (current version %d)", e.Op, e.Key, e.CurrentVersion)
}

// NewVersionMismatchError creates a VersionMismatchError.
func NewVersionMismatchError(op, key string, current uint64) *VersionMismatchError {
	return &VersionMismatchError{Op: op, Key: key, CurrentVersion: current}
}

// IsVersionMismatch reports whether err is (or wraps) a VersionMismatchError,
// returning it for access to CurrentVersion.
func IsVersionMismatch(err error) (*VersionMismatchError, bool) {
	var vm *VersionMismatchError
	if errors.As(err, &vm) {
		return vm, true
	}
	return nil, false
}
