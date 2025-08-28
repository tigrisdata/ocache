package errors

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetriable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"EAGAIN", syscall.EAGAIN, true},
		{"EINTR", syscall.EINTR, true},
		{"ErrTemporary", ErrTemporary, true},
		{"ErrLocked", ErrLocked, true},
		{"short write", io.ErrShortWrite, true},
		{"too many files", syscall.EMFILE, true},
		{"ErrTooManyFiles", ErrTooManyFiles, true},
		{"temporary in string", errors.New("temporarily unavailable"), true},
		{"write stall", errors.New("write stall detected"), true},
		{"EOF", io.EOF, false},
		{"not found", ErrKeyNotFound, false},
		{"permission denied", os.ErrPermission, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRetriable(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToGRPCCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		// Client errors
		{"ErrKeyNotFound", ErrKeyNotFound, codes.NotFound},
		{"os.ErrNotExist", os.ErrNotExist, codes.NotFound},
		{"ENOENT", syscall.ENOENT, codes.NotFound},
		{"ErrInvalidKey", ErrInvalidKey, codes.InvalidArgument},
		{"ErrInvalidByteRange", ErrInvalidByteRange, codes.InvalidArgument},
		{"EOF", io.EOF, codes.OutOfRange},
		{"context.Canceled", context.Canceled, codes.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded, codes.DeadlineExceeded},

		// Permission errors
		{"os.ErrPermission", os.ErrPermission, codes.PermissionDenied},
		{"EACCES", syscall.EACCES, codes.PermissionDenied},

		// Resource errors
		{"ErrDiskFull", ErrDiskFull, codes.ResourceExhausted},
		{"ENOSPC", syscall.ENOSPC, codes.ResourceExhausted},
		{"ErrTooManyFiles", ErrTooManyFiles, codes.ResourceExhausted},
		{"EMFILE", syscall.EMFILE, codes.ResourceExhausted},

		// Data corruption
		{"ErrCorrupted", ErrCorrupted, codes.DataLoss},
		{"unexpected EOF", io.ErrUnexpectedEOF, codes.DataLoss},
		{"corruption string", errors.New("corruption detected"), codes.DataLoss},
		{"checksum error", errors.New("checksum mismatch"), codes.DataLoss},

		// Transient errors
		{"ErrTemporary", ErrTemporary, codes.Unavailable},
		{"ErrLocked", ErrLocked, codes.Unavailable},
		{"EAGAIN", syscall.EAGAIN, codes.Unavailable},

		// I/O errors
		{"EIO", syscall.EIO, codes.Internal},

		// String matching
		{"not found string", errors.New("key not found in database"), codes.NotFound},
		{"already exists", errors.New("file already exists"), codes.AlreadyExists},
		{"permission string", errors.New("permission denied"), codes.PermissionDenied},
		{"no space string", errors.New("no space left"), codes.ResourceExhausted},

		// Unknown
		{"unknown", errors.New("something went wrong"), codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toGRPCCode(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToGRPCError(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		assert.Nil(t, ToGRPCError(nil))
	})

	t.Run("already gRPC status", func(t *testing.T) {
		grpcErr := status.Error(codes.NotFound, "test")
		result := ToGRPCError(grpcErr)
		assert.Equal(t, grpcErr, result)
	})

	t.Run("converts to gRPC status", func(t *testing.T) {
		err := ErrKeyNotFound
		result := ToGRPCError(err)

		st, ok := status.FromError(result)
		assert.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Equal(t, "key not found", st.Message())
	})

	t.Run("preserves error message", func(t *testing.T) {
		err := errors.New("custom error message")
		result := ToGRPCError(err)

		st, ok := status.FromError(result)
		assert.True(t, ok)
		assert.Equal(t, codes.Unknown, st.Code())
		assert.Equal(t, "custom error message", st.Message())
	})
}

func TestIsDataCorruption(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ErrCorrupted", ErrCorrupted, true},
		{"ErrFileCorrupted", ErrFileCorrupted, true},
		{"ErrMalformedIndexRow", ErrMalformedIndexRow, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"corruption in string", errors.New("data corruption detected"), true},
		{"checksum error", errors.New("checksum mismatch"), true},
		{"normal error", errors.New("something else"), false},
		{"EOF", io.EOF, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDataCorruption(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsResourceExhausted(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ErrDiskFull", ErrDiskFull, true},
		{"ErrTooManyFiles", ErrTooManyFiles, true},
		{"ENOSPC", syscall.ENOSPC, true},
		{"EMFILE", syscall.EMFILE, true},
		{"ENFILE", syscall.ENFILE, true},
		{"no space in string", errors.New("no space left on device"), true},
		{"disk full in string", errors.New("disk full"), true},
		{"too many in string", errors.New("too many open files"), true},
		{"normal error", errors.New("something else"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsResourceExhausted(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWrap(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		key      string
		err      error
		expected string
		isNil    bool
	}{
		{
			name:  "nil error",
			op:    "get",
			key:   "test",
			err:   nil,
			isNil: true,
		},
		{
			name:     "with key",
			op:       "get",
			key:      "test-key",
			err:      io.EOF,
			expected: "get test-key: EOF",
		},
		{
			name:     "without key",
			op:       "read",
			key:      "",
			err:      io.EOF,
			expected: "read: EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Wrap(tt.op, tt.key, tt.err)
			if tt.isNil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expected, result.Error())
				// Check that wrapped error is preserved
				assert.True(t, errors.Is(result, tt.err))
			}
		})
	}
}
