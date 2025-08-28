package errors

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// I/O error
var ErrIO = errors.New("io: operation failed")

// IOError wraps an I/O error with context
type IOError struct {
	Op   string
	Path string
	Err  error
}

func (e *IOError) Error() string {
	return fmt.Sprintf("%s %q: %v", e.Op, e.Path, e.Err)
}

func (e *IOError) Unwrap() error {
	return e.Err
}

// WrapIOError wraps an I/O error and returns appropriate specific error type
func WrapIOError(op, path string, err error) error {
	if err == nil {
		return nil
	}

	// Map to specific error types for special cases
	var baseErr error
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, ErrFileNotExist):
		baseErr = ErrFileNotExist
	case errors.Is(err, syscall.ENOSPC):
		baseErr = ErrDiskFull
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		baseErr = ErrTooManyFiles
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, ErrCorrupted):
		baseErr = ErrCorrupted
	case errors.Is(err, os.ErrPermission):
		return err // Return permission error as-is
	case errors.Is(err, ErrInvalidKey):
		baseErr = err
	default:
		baseErr = ErrIO
	}

	// Return wrapped error with context
	return &IOError{
		Op:   op,
		Path: path,
		Err:  baseErr,
	}
}

// IsIOError checks if an error is an I/O error
func IsIOError(err error) bool {
	return errors.Is(err, ErrIO)
}

// IsIORetriable checks if an I/O error is retriable
func IsIORetriable(err error) bool {
	if err == nil {
		return false
	}

	// Check for non-retriable conditions
	switch {
	case errors.Is(err, os.ErrPermission):
		return false
	case errors.Is(err, ErrCorrupted):
		return false
	case errors.Is(err, ErrDiskFull):
		return false
	case errors.Is(err, ErrFileNotExist):
		return false
	case errors.Is(err, io.EOF):
		return false
	default:
		return IsIOError(err)
	}
}

// ErrFileSizeMismatch is returned when a file's actual size doesn't match its metadata
type ErrFileSizeMismatch struct {
	Key          string
	FilePath     string
	ActualSize   int64
	ExpectedSize int64
}

func (e *ErrFileSizeMismatch) Error() string {
	return fmt.Sprintf("file size mismatch for key %s: actual=%d expected=%d", e.Key, e.ActualSize, e.ExpectedSize)
}
