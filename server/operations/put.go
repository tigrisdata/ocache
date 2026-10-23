// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"io"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultStreamBufferSize is the default buffer size for streaming operations
	DefaultStreamBufferSize = 1 << 20 // 1 MiB
)

// PutOptions tunes a single Put.
type PutOptions struct {
	// ShedOverBudget refuses the write with an over-budget error (gRPC
	// ResourceExhausted; see IsOverBudget) instead of storing it when the OWNING
	// node is at or over its disk shed ratio — evaluated at the owner, so a
	// forwarding node cannot fill an overloaded peer. Meant for bodies the caller
	// can serve without caching; never for metadata the caller relies on.
	ShedOverBudget bool
}

// Put stores data for the given key with automatic routing.
// If the key is local, it stores directly in storage.
// If the key is remote, it sends via gRPC to the appropriate node.
func (o *Operations) Put(ctx context.Context, key string, body io.Reader, ttl int) error {
	return o.PutWithOptions(ctx, key, body, ttl, PutOptions{})
}

// PutWithOptions is Put with per-call options.
func (o *Operations) PutWithOptions(ctx context.Context, key string, body io.Reader, ttl int, opts PutOptions) error {
	done := recordOperationStart("Operations.Put")

	zlog.Debug().Str("key", key).Int("ttl", ttl).Msg("Operations.Put called")

	var err error
	if o.IsLocal(key) {
		err = o.PutLocalWithOptions(ctx, key, body, ttl, opts)
	} else {
		// Remote key - send via gRPC
		err = o.putRemote(ctx, key, body, ttl, opts)
	}

	done(err)
	return err
}

// PutLocal stores data in local storage directly.
// If the body implements io.Seeker (e.g., bytes.Reader), retries are enabled with automatic reset.
// For non-seekable readers (e.g., io.PipeReader from streaming), no retries are attempted
// since the reader cannot be rewound.
func (o *Operations) PutLocal(ctx context.Context, key string, body io.Reader, ttl int) error {
	return o.PutLocalWithOptions(ctx, key, body, ttl, PutOptions{})
}

// PutLocalWithOptions is PutLocal with per-call options. A sheddable write is
// refused BEFORE the body is read, so the caller still holds its bytes.
func (o *Operations) PutLocalWithOptions(ctx context.Context, key string, body io.Reader, ttl int, opts PutOptions) error {
	if opts.ShedOverBudget && o.storage.OverShedRatio() {
		recordShed("Put")
		return storageErrors.NewOverBudgetError("Put")
	}

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
func (o *Operations) putRemote(ctx context.Context, key string, body io.Reader, ttl int, opts PutOptions) error {
	// Increment hop count for forwarding loop detection
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for put")
		return err
	}

	client, err := o.RouteContext(ctx, key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key for put")
		return err
	}

	stream, err := client.Put(ctx)
	if err != nil {
		return err
	}

	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultStreamBufferSize)
	defer release()

	// Always send the first message with key and TTL, even if data is empty
	firstReq := &pb.PutRequest{
		Key:            key,
		TtlSeconds:     int64(ttl),
		ShedOverBudget: opts.ShedOverBudget,
	}

	// Read first chunk of data
	n, err := body.Read(buf)
	if n > 0 {
		firstReq.Data = buf[:n]
	}
	if sendErr := stream.Send(firstReq); sendErr != nil {
		return sendErr
	}
	if n > 0 {
		recordBytesTransferred("upload", int64(n))
	}

	// If first read hit EOF, we're done (handles empty data case)
	if err == io.EOF {
		// Continue to CloseAndRecv below
	} else if err != nil {
		return err
	} else {
		// Stream remaining data
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n, err := body.Read(buf)
			if n > 0 {
				req := &pb.PutRequest{Data: buf[:n]}
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
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return &PutError{Message: resp.Error, Code: codes.Code(resp.Code)}
	}
	return nil
}

// IsOverBudget reports whether err is a refused sheddable write, whether it
// came from the local store or from the owning node over gRPC.
func IsOverBudget(err error) bool {
	if err == nil {
		return false
	}
	if storageErrors.IsOverBudget(err) {
		return true
	}
	return status.Code(err) == codes.ResourceExhausted
}

// PutBytes stores a byte slice for the given key with automatic routing.
// This is a convenience method that wraps the byte slice in a reader.
func (o *Operations) PutBytes(ctx context.Context, key string, data []byte, ttl int) error {
	return o.Put(ctx, key, bytes.NewReader(data), ttl)
}

// PutError represents an error from a Put operation on a remote node.
type PutError struct {
	Message string
	// Code is the owner's gRPC status code for the failure (OK when the owner
	// predates the field); GRPCStatus exposes it to status.Code.
	Code codes.Code
}

func (e *PutError) Error() string {
	return e.Message
}

// GRPCStatus lets status.Code(err) see the owner's code through the
// PutResponse relay, which stringifies the original status.
func (e *PutError) GRPCStatus() *status.Status {
	if e.Code == codes.OK {
		return status.New(codes.Unknown, e.Message)
	}
	return status.New(e.Code, e.Message)
}
