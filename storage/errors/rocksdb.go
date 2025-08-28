package errors

import (
	"errors"
	"fmt"
	"strings"
)

// RocksDB-specific errors
var (
	// ErrRocksDBCorrupted indicates RocksDB data corruption
	ErrRocksDBCorrupted = errors.New("rocksdb: data corrupted")

	// ErrRocksDBIOError indicates RocksDB I/O failure
	ErrRocksDBIOError = errors.New("rocksdb: I/O error")

	// ErrRocksDBNotFound indicates key not found in RocksDB
	ErrRocksDBNotFound = errors.New("rocksdb: not found")

	// ErrRocksDBFull indicates RocksDB is full (disk space)
	ErrRocksDBFull = errors.New("rocksdb: database full")

	// ErrRocksDBInvalidArgument indicates invalid argument to RocksDB
	ErrRocksDBInvalidArgument = errors.New("rocksdb: invalid argument")

	// ErrRocksDBMergeInProgress indicates merge operation in progress
	ErrRocksDBMergeInProgress = errors.New("rocksdb: merge in progress")

	// ErrRocksDBIncomplete indicates incomplete operation
	ErrRocksDBIncomplete = errors.New("rocksdb: incomplete")

	// ErrRocksDBShutdownInProgress indicates shutdown in progress
	ErrRocksDBShutdownInProgress = errors.New("rocksdb: shutdown in progress")

	// ErrRocksDBTimeout indicates operation timed out
	ErrRocksDBTimeout = errors.New("rocksdb: timed out")

	// ErrRocksDBAborted indicates operation was aborted
	ErrRocksDBAborted = errors.New("rocksdb: aborted")

	// ErrRocksDBBusy indicates resource is busy
	ErrRocksDBBusy = errors.New("rocksdb: busy")

	// ErrRocksDBExpired indicates resource has expired
	ErrRocksDBExpired = errors.New("rocksdb: expired")

	// ErrRocksDBTryAgain indicates operation should be retried
	ErrRocksDBTryAgain = errors.New("rocksdb: try again")

	// ErrRocksDBGeneric indicates a generic RocksDB error
	ErrRocksDBGeneric = errors.New("rocksdb: error")
)

type RocksDBError struct {
	Op  string
	Key string
	Err error
}

func (e *RocksDBError) Error() string {
	return fmt.Sprintf("%s %q: %v", e.Op, e.Key, e.Err)
}

func (e *RocksDBError) Unwrap() error {
	return e.Err
}

// WrapRocksDBError wraps a RocksDB error with context and maps it to appropriate error type
func WrapRocksDBError(op, key string, err error) error {
	if err == nil {
		return nil
	}

	// Map RocksDB error messages to our error types
	errStr := err.Error()

	// Check for specific RocksDB error patterns
	var baseErr error
	switch {
	case strings.Contains(errStr, "NotFound"):
		baseErr = ErrRocksDBNotFound
	case strings.Contains(errStr, "Corruption"):
		baseErr = ErrRocksDBCorrupted
	case strings.Contains(errStr, "IO error"):
		baseErr = ErrRocksDBIOError
	case strings.Contains(errStr, "No space left"):
		baseErr = ErrRocksDBFull
	case strings.Contains(errStr, "Invalid argument"):
		baseErr = ErrRocksDBInvalidArgument
	case strings.Contains(errStr, "Merge in progress"):
		baseErr = ErrRocksDBMergeInProgress
	case strings.Contains(errStr, "Incomplete"):
		baseErr = ErrRocksDBIncomplete
	case strings.Contains(errStr, "Shutdown in progress"):
		baseErr = ErrRocksDBShutdownInProgress
	case strings.Contains(errStr, "Timeout"):
		baseErr = ErrRocksDBTimeout
	case strings.Contains(errStr, "Aborted"):
		baseErr = ErrRocksDBAborted
	case strings.Contains(errStr, "Resource busy"):
		baseErr = ErrRocksDBBusy
	case strings.Contains(errStr, "Expired"):
		baseErr = ErrRocksDBExpired
	case strings.Contains(errStr, "TryAgain"):
		baseErr = ErrRocksDBTryAgain
	default:
		// Generic RocksDB error
		baseErr = ErrRocksDBGeneric
	}

	return &RocksDBError{
		Op:  op,
		Key: key,
		Err: baseErr,
	}
}

// IsRocksDBError checks if an error is a RocksDB error
func IsRocksDBError(err error) bool {
	return errors.Is(err, ErrRocksDBCorrupted) ||
		errors.Is(err, ErrRocksDBIOError) ||
		errors.Is(err, ErrRocksDBNotFound) ||
		errors.Is(err, ErrRocksDBFull) ||
		errors.Is(err, ErrRocksDBInvalidArgument) ||
		errors.Is(err, ErrRocksDBMergeInProgress) ||
		errors.Is(err, ErrRocksDBIncomplete) ||
		errors.Is(err, ErrRocksDBShutdownInProgress) ||
		errors.Is(err, ErrRocksDBTimeout) ||
		errors.Is(err, ErrRocksDBAborted) ||
		errors.Is(err, ErrRocksDBBusy) ||
		errors.Is(err, ErrRocksDBExpired) ||
		errors.Is(err, ErrRocksDBTryAgain)
}

// IsRocksDBRetriable checks if a RocksDB error is retriable
func IsRocksDBRetriable(err error) bool {
	return errors.Is(err, ErrRocksDBIOError) ||
		errors.Is(err, ErrRocksDBMergeInProgress) ||
		errors.Is(err, ErrRocksDBIncomplete) ||
		errors.Is(err, ErrRocksDBTimeout) ||
		errors.Is(err, ErrRocksDBBusy) ||
		errors.Is(err, ErrRocksDBTryAgain)
}

// IsRocksDBCorruption checks if a RocksDB error indicates data corruption
func IsRocksDBCorruption(err error) bool {
	return errors.Is(err, ErrRocksDBCorrupted)
}
