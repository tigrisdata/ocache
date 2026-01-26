package operations

import (
	"bytes"
	"context"
	"io"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
)

const (
	// DefaultStreamBufferSize is the default buffer size for streaming operations
	DefaultStreamBufferSize = 1 << 20 // 1 MiB
)

// Put stores data for the given key with automatic routing.
// If the key is local, it stores directly in storage.
// If the key is remote, it sends via gRPC to the appropriate node.
func (o *Operations) Put(ctx context.Context, key string, body io.Reader, ttl int) error {
	done := recordOperationStart("Operations.Put")

	zlog.Debug().Str("key", key).Int("ttl", ttl).Msg("Operations.Put called")

	var err error
	if o.IsLocal(key) {
		err = o.PutLocal(ctx, key, body, ttl)
	} else {
		// Remote key - send via gRPC
		err = o.putRemote(ctx, key, body, ttl)
	}

	done(err)
	return err
}

// PutLocal stores data in local storage directly.
// If the body implements io.Seeker (e.g., bytes.Reader), retries are enabled with automatic reset.
// For non-seekable readers (e.g., io.PipeReader from streaming), no retries are attempted
// since the reader cannot be rewound.
func (o *Operations) PutLocal(ctx context.Context, key string, body io.Reader, ttl int) error {
	// Check if we can retry (reader is seekable)
	seeker, canRetry := body.(io.Seeker)

	if !canRetry {
		// Non-seekable reader (e.g., io.PipeReader) - no retry possible
		return o.storage.Put(key, body, ttl)
	}

	// Seekable reader (e.g., bytes.Reader) - retry with reset
	return retry.DoWithKey(ctx, retry.DefaultConfig(), "Put", key, func() error {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return o.storage.Put(key, body, ttl)
	})
}

// putRemote sends data to a remote node via gRPC streaming.
func (o *Operations) putRemote(ctx context.Context, key string, body io.Reader, ttl int) error {
	// Increment hop count for forwarding loop detection
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		zlog.Warn().Err(err).Str("key", key).Msg("Hop count limit exceeded for put")
		return err
	}

	client, err := o.Route(key)
	if err != nil {
		zlog.Warn().Err(err).Str("key", key).Msg("Failed to route key for put")
		return err
	}

	stream, err := client.Put(ctx)
	if err != nil {
		return err
	}

	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultStreamBufferSize)
	defer release()

	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := body.Read(buf)
		if n > 0 {
			req := &pb.PutRequest{Data: buf[:n]}
			if first {
				req.Key = key
				req.TtlSeconds = int64(ttl)
				first = false
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return sendErr
			}
			recordBytesTransferred("upload", int64(n))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return &PutError{Message: resp.Error}
	}
	return nil
}

// PutBytes stores a byte slice for the given key with automatic routing.
// This is a convenience method that wraps the byte slice in a reader.
func (o *Operations) PutBytes(ctx context.Context, key string, data []byte, ttl int) error {
	return o.Put(ctx, key, bytes.NewReader(data), ttl)
}

// PutError represents an error from a Put operation.
type PutError struct {
	Message string
}

func (e *PutError) Error() string {
	return e.Message
}
