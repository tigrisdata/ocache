// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"errors"
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
	// ShedOverBudget refuses the write with an over-budget error (see
	// IsOverBudget) instead of storing it when the OWNING node is at or over its
	// disk shed ratio — evaluated at the owner, so a forwarding node cannot fill
	// an overloaded peer. Meant for bodies the caller can serve without caching;
	// never for metadata the caller relies on. A LOCAL owner refuses before the
	// body is read; a REMOTE owner answers over the stream, so part of the body
	// may already have been read and sent when the refusal arrives. Treat a
	// refusal as "not stored" and never replay the reader.
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

	if IsOverBudget(err) {
		done(err, "shed") // a refusal the caller asked for, not a failure
	} else {
		done(err)
	}
	return err
}

// OverBudget reports whether this node's store is at or over its disk shed
// ratio, i.e. whether a sheddable write would be refused right now.
func (o *Operations) OverBudget() bool {
	return o.storage.OverShedRatio()
}

// PutLocal stores data in local storage directly. If the reader's current
// position can be obtained through io.Seeker, retryable failures restart from
// that call-entry position. Otherwise, storage is attempted once from the
// reader's current position.
func (o *Operations) PutLocal(ctx context.Context, key string, body io.Reader, ttl int) error {
	return o.PutLocalWithOptions(ctx, key, body, ttl, PutOptions{})
}

// PutLocalWithOptions is PutLocal with per-call options. A sheddable write the
// local store refuses is refused BEFORE the body is read.
func (o *Operations) PutLocalWithOptions(ctx context.Context, key string, body io.Reader, ttl int, opts PutOptions) error {
	if opts.ShedOverBudget && o.storage.OverShedRatio() {
		return storageErrors.NewOverBudgetError("Put")
	}

	return putLocalWithRetry(ctx, key, body, func() error {
		return o.storage.Put(key, body, ttl)
	})
}

// putLocalWithRetry attempts an unpositionable reader once. When its initial
// offset is available, the first attempt reads from the current position and
// each retry restores that same offset.
func putLocalWithRetry(ctx context.Context, key string, body io.Reader, put func() error) error {
	seeker, ok := body.(io.Seeker)
	if !ok {
		return put()
	}

	start, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil || start < 0 {
		return put()
	}

	attempt := 0
	return retry.DoWithKey(ctx, retry.DefaultConfig(), "Put", key, func() error {
		if attempt > 0 {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				if storageErrors.IsRetryable(err) {
					// A failed reposition is terminal even if its error resembles a storage error.
					return errors.New(err.Error())
				}
				return err
			}
		}
		attempt++
		return put()
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

	resp, err := SendPutStream(ctx, stream, &pb.PutRequest{
		Key:            key,
		TtlSeconds:     int64(ttl),
		ShedOverBudget: opts.ShedOverBudget,
	}, body)
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return &PutError{Message: resp.Error, Code: codes.Code(resp.Code), Shed: resp.Shed}
	}
	return nil
}

// SendPutStream streams first (key, ttl, options) and then body in chunks over
// an open Put call and returns the owner's response. A sheddable first message
// carries no body, so the owner can refuse on the key alone before any body
// byte is on the wire. A Send returning io.EOF means the owner ended the RPC
// early (such a refusal), so sending stops and CloseAndRecv surfaces the
// in-band outcome instead of the send error that would hide it. ctx is checked
// before every body read; cancellation does not interrupt a read already in
// progress, so a caller with a blocking reader must also close that reader.
func SendPutStream(ctx context.Context, stream pb.CacheService_PutClient, first *pb.PutRequest, body io.Reader) (*pb.PutResponse, error) {
	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultStreamBufferSize)
	defer release()

	readChunk := func() (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			return n, err // local read failure — not a stream/server issue
		}
		return n, err
	}

	// Always send the first message with key and TTL. It carries the first
	// chunk too, except for a sheddable write, whose owner decides on the key.
	var n int
	var readErr error
	if !first.ShedOverBudget {
		if n, readErr = readChunk(); readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if n > 0 {
			first.Data = buf[:n]
		}
	}
	sendErr := stream.Send(first)
	if sendErr == nil && n > 0 {
		recordBytesTransferred("upload", int64(n))
	}
	for sendErr == nil && readErr != io.EOF {
		if n, readErr = readChunk(); readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if n > 0 {
			if sendErr = stream.Send(&pb.PutRequest{Data: buf[:n]}); sendErr == nil {
				recordBytesTransferred("upload", int64(n))
			}
		}
	}
	if sendErr != nil && sendErr != io.EOF {
		return nil, sendErr
	}
	return stream.CloseAndRecv()
}

// IsOverBudget reports whether err is a sheddable write refused by its owner,
// whether that owner was the local store or a peer over gRPC. Only the explicit
// refusal marker counts: a genuine storage-full or hop-limit failure also maps
// to ResourceExhausted and must stay an error.
func IsOverBudget(err error) bool {
	if err == nil {
		return false
	}
	if storageErrors.IsOverBudget(err) {
		return true
	}
	var pe *PutError
	return errors.As(err, &pe) && pe.Shed
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
	// Shed marks a refused sheddable write (see PutOptions.ShedOverBudget).
	Shed bool
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
