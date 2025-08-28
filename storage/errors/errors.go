package errors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Common storage errors
var (
	// Client errors (4xx)
	ErrKeyNotFound      = errors.New("key not found")
	ErrInvalidKey       = errors.New("invalid key")
	ErrInvalidByteRange = errors.New("invalid byte range")
	ErrMetadataNotFound = errors.New("metadata not found")
	ErrNotRawFile       = errors.New("not raw file")

	// Server errors (5xx)
	ErrCorrupted         = errors.New("data corrupted")
	ErrDiskFull          = errors.New("disk full")
	ErrTooManyFiles      = errors.New("too many open files")
	ErrFileCorrupted     = errors.New("file corrupted")
	ErrFilePathMismatch  = errors.New("file path mismatch")
	ErrMalformedIndexRow = errors.New("malformed index row")
	ErrFileNotExist      = errors.New("file does not exist")
	ErrFileClosed        = errors.New("file is closed")

	// Transient errors (retriable)
	ErrTemporary        = errors.New("temporary failure")
	ErrLocked           = errors.New("resource locked")
	ErrAlreadyCompacted = errors.New("already compacted")
	ErrEntryStale       = errors.New("sync entry is stale")
)

type BaseError struct {
	Op  string
	Key string
	Err error
}

func (e *BaseError) Error() string {
	return fmt.Sprintf("%s %q: %v", e.Op, e.Key, e.Err)
}

func (e *BaseError) Unwrap() error {
	return e.Err
}

// IsRetriable returns true if the error should be retried
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}

	// Check for RocksDB retriable errors
	if IsRocksDBRetriable(err) {
		return true
	}

	// Check for I/O retriable errors
	if IsIORetriable(err) {
		return true
	}

	// Check for known retriable errors
	switch {
	case errors.Is(err, syscall.EAGAIN),
		errors.Is(err, syscall.EINTR),
		errors.Is(err, ErrTemporary),
		errors.Is(err, ErrLocked),
		errors.Is(err, io.ErrShortWrite):
		return true

	case errors.Is(err, syscall.EMFILE),
		errors.Is(err, syscall.ENFILE),
		errors.Is(err, ErrTooManyFiles):
		return true // Can retry after closing some files

	default:
		// Check for retriable patterns in error strings
		errStr := err.Error()
		return strings.Contains(errStr, "temporarily") ||
			strings.Contains(errStr, "write stall") ||
			strings.Contains(errStr, "too many open files")
	}
}

// ToGRPCError converts a storage error to appropriate gRPC status
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}

	// Already a gRPC status error
	if _, ok := status.FromError(err); ok {
		return err
	}

	code := toGRPCCode(err)
	return status.Error(code, err.Error())
}

// toGRPCCode maps errors to gRPC codes
func toGRPCCode(err error) codes.Code {
	switch {
	// Client errors
	case errors.Is(err, ErrKeyNotFound),
		errors.Is(err, ErrMetadataNotFound),
		errors.Is(err, ErrRocksDBNotFound),
		errors.Is(err, os.ErrNotExist),
		errors.Is(err, syscall.ENOENT):
		return codes.NotFound

	case errors.Is(err, ErrInvalidKey),
		errors.Is(err, ErrInvalidByteRange),
		errors.Is(err, ErrRocksDBInvalidArgument):
		return codes.InvalidArgument

	case errors.Is(err, io.EOF):
		return codes.OutOfRange

	case errors.Is(err, context.Canceled):
		return codes.Canceled

	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded

	// Permission errors
	case errors.Is(err, os.ErrPermission),
		errors.Is(err, syscall.EACCES),
		errors.Is(err, syscall.EPERM):
		return codes.PermissionDenied

	// Resource errors
	case errors.Is(err, ErrDiskFull),
		errors.Is(err, ErrRocksDBFull),
		errors.Is(err, syscall.ENOSPC),
		errors.Is(err, ErrTooManyFiles),
		errors.Is(err, syscall.EMFILE),
		errors.Is(err, syscall.ENFILE):
		return codes.ResourceExhausted

	// Data corruption
	case errors.Is(err, ErrCorrupted),
		errors.Is(err, ErrFileCorrupted),
		errors.Is(err, ErrMalformedIndexRow),
		errors.Is(err, ErrRocksDBCorrupted),
		errors.Is(err, io.ErrUnexpectedEOF),
		strings.Contains(err.Error(), "corruption"),
		strings.Contains(err.Error(), "checksum"):
		return codes.DataLoss

	// Transient/Unavailable
	case errors.Is(err, ErrTemporary),
		errors.Is(err, ErrLocked),
		errors.Is(err, ErrRocksDBBusy),
		errors.Is(err, ErrRocksDBTryAgain),
		errors.Is(err, syscall.EAGAIN),
		errors.Is(err, syscall.EINTR):
		return codes.Unavailable

	// I/O errors
	case errors.Is(err, syscall.EIO),
		errors.Is(err, ErrRocksDBIOError),
		IsIOError(err):
		return codes.Internal

	default:
		// Check error strings for patterns
		errStr := strings.ToLower(err.Error())
		switch {
		case strings.Contains(errStr, "not found"):
			return codes.NotFound
		case strings.Contains(errStr, "already exists"):
			return codes.AlreadyExists
		case strings.Contains(errStr, "permission"):
			return codes.PermissionDenied
		case strings.Contains(errStr, "no space"):
			return codes.ResourceExhausted
		default:
			return codes.Unknown
		}
	}
}

// IsDataCorruption returns true if the error indicates data corruption
func IsDataCorruption(err error) bool {
	if err == nil {
		return false
	}

	// Check for RocksDB corruption
	if IsRocksDBCorruption(err) {
		return true
	}

	// Check for known corruption errors
	switch {
	case errors.Is(err, ErrCorrupted),
		errors.Is(err, ErrFileCorrupted),
		errors.Is(err, ErrMalformedIndexRow),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	default:
		// Check error message for corruption patterns
		errStr := strings.ToLower(err.Error())
		return strings.Contains(errStr, "corruption") ||
			strings.Contains(errStr, "corrupted") ||
			strings.Contains(errStr, "checksum")
	}
}

// IsResourceExhausted returns true if the error indicates resource exhaustion
func IsResourceExhausted(err error) bool {
	if err == nil {
		return false
	}

	// Check for known resource errors
	switch {
	case errors.Is(err, ErrDiskFull),
		errors.Is(err, ErrTooManyFiles),
		errors.Is(err, syscall.ENOSPC),
		errors.Is(err, syscall.EMFILE),
		errors.Is(err, syscall.ENFILE):
		return true
	default:
		// Check error message for resource patterns
		errStr := strings.ToLower(err.Error())
		return strings.Contains(errStr, "no space") ||
			strings.Contains(errStr, "disk full") ||
			strings.Contains(errStr, "too many")
	}
}

// Wrap wraps an error with context information
func Wrap(op, key string, err error) error {
	if err == nil {
		return nil
	}

	return &BaseError{
		Op:  op,
		Key: key,
		Err: err,
	}
}
