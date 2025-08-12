package utils

import (
	"errors"
	"fmt"
)

// WrapError wraps an error with an operation and path.
func WrapError(op string, path string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %s: %w", op, path, err)
}

// Sentinel errors for stale file validation
var (
	// ErrMetadataNotFound indicates the metadata doesn't exist
	ErrMetadataNotFound = errors.New("metadata not found")

	// ErrAlreadyCompacted indicates the file has already been compacted to a segment
	ErrAlreadyCompacted = errors.New("already compacted")

	// ErrNotRawFile indicates the value is not a raw file (could be inline or other type)
	ErrNotRawFile = errors.New("not raw file")

	// ErrFilePathMismatch indicates the metadata points to a different file
	ErrFilePathMismatch = errors.New("file path mismatch")

	// ErrMalformedIndexRow indicates the compaction index row is malformed
	ErrMalformedIndexRow = errors.New("malformed index row")

	// ErrFileNotExist indicates the file doesn't exist on disk
	ErrFileNotExist = errors.New("file not exist")
)
