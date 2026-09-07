// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"io"

	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/retry"
)

// Conditional (compare-and-swap) operations with automatic routing (issue #254).
// A version mismatch is carried as *storageErrors.VersionMismatchError so the
// contract is uniform across the local and remote paths and up into the service
// layer. CAS is deliberately NOT wrapped in the blind retry the other ops use:
// a retried conditional write is not idempotent (it would issue a fresh stamp
// and could mismatch after an outcome it actually won). The storage layer's own
// read-back retry already absorbs transient read glitches.

// GetWithVersion returns key's value bytes and current CAS version. A read is
// idempotent, so the local open is wrapped in the same retry as GetLocal — the
// storage read path returns retryable lock/IO errors (file locks, the brief
// raw->segment compaction unlink window, a dangling-file self-heal) that a
// re-read recovers from; without this a CAS read would spuriously miss a key a
// plain Get would return. (The CAS *writes* still skip retry — they are not
// idempotent.)
func (o *Operations) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if o.IsLocal(key) {
		r, version, found, err := o.GetReaderWithVersionLocal(ctx, key)
		if err != nil || !found {
			return nil, version, found, err
		}
		data, readErr := io.ReadAll(r)
		if rc, ok := r.(io.ReadCloser); ok {
			_ = rc.Close()
		}
		if readErr != nil {
			return nil, 0, false, readErr
		}
		return data, version, true, nil
	}
	return o.getWithVersionRemote(ctx, key)
}

// GetReaderWithVersionLocal returns a streaming reader for a local key together
// with its CAS version, without buffering the value — used by the streaming
// GetStreamWithVersion handler for large objects (issue #258). The open is
// retried like GetLocal (the read is idempotent). The caller must Close the
// reader if it is an io.Closer.
func (o *Operations) GetReaderWithVersionLocal(ctx context.Context, key string) (io.Reader, uint64, bool, error) {
	var r io.Reader
	var version uint64
	var found bool
	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "GetWithVersion", key, func() error {
		var getErr error
		r, version, found, getErr = o.storage.GetWithVersion(key)
		return getErr
	})
	if err != nil {
		return nil, 0, false, err
	}
	return r, version, found, nil
}

// PutStreamIfVersion streams a value to the owner (local storage or a remote
// node) and applies it only if the version matches — the routing entry point
// used by the embedded client for large objects (issue #258).
func (o *Operations) PutStreamIfVersion(ctx context.Context, key string, r io.Reader, ttl int, expected uint64) (uint64, error) {
	if o.IsLocal(key) {
		return o.PutStreamIfVersionLocal(ctx, key, r, ttl, expected)
	}
	return o.putStreamIfVersionRemote(ctx, key, r, ttl, expected)
}

// GetStreamWithVersion writes a value to w and returns its CAS version, without
// buffering — the routing entry point used by the embedded client.
func (o *Operations) GetStreamWithVersion(ctx context.Context, key string, w io.Writer) (uint64, bool, error) {
	if o.IsLocal(key) {
		r, version, found, err := o.GetReaderWithVersionLocal(ctx, key)
		if err != nil || !found {
			return version, found, err
		}
		if rc, ok := r.(io.ReadCloser); ok {
			defer rc.Close()
		}
		if _, err := io.Copy(w, r); err != nil {
			return 0, false, err
		}
		return version, true, nil
	}
	return o.getStreamWithVersionRemote(ctx, key, w)
}

func (o *Operations) putStreamIfVersionRemote(ctx context.Context, key string, r io.Reader, ttl int, expected uint64) (uint64, error) {
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		return 0, err
	}
	client, err := o.Route(key)
	if err != nil {
		return 0, err
	}
	stream, err := client.PutStreamIfVersion(ctx)
	if err != nil {
		return 0, err
	}
	// A Send returning io.EOF means the owner ended the RPC early — typically a
	// mismatch it resolved before consuming the body. Stop sending and let
	// CloseAndRecv surface the in-band outcome instead of returning the send
	// error, which would hide the mismatch the caller must retry against.
	sendErr := stream.Send(&pb.PutIfVersionRequest{Key: key, TtlSeconds: int64(ttl), ExpectedVersion: expected})
	if sendErr == nil {
		// Same chunk size and buffer pool as the plain streaming put.
		buf, release := bufferpool.AcquireBuffer(DefaultStreamBufferSize)
		defer release()
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				if err := stream.Send(&pb.PutIfVersionRequest{Data: buf[:n]}); err != nil {
					sendErr = err
					break
				}
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return 0, rerr // local read failure — not a stream/server issue
			}
		}
	}
	if sendErr != nil && sendErr != io.EOF {
		return 0, sendErr
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, &operationError{message: resp.Error}
	}
	if !resp.Success {
		return 0, storageErrors.NewVersionMismatchError("PutStreamIfVersion", key, resp.CurrentVersion)
	}
	return resp.NewVersion, nil
}

func (o *Operations) getStreamWithVersionRemote(ctx context.Context, key string, w io.Writer) (uint64, bool, error) {
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		return 0, false, err
	}
	client, err := o.Route(key)
	if err != nil {
		return 0, false, err
	}
	stream, err := client.GetStreamWithVersion(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return 0, false, err
	}
	first, err := stream.Recv()
	if err != nil {
		return 0, false, err
	}
	if !first.Found {
		return first.Version, false, nil
	}
	version := first.Version
	if len(first.Data) > 0 {
		if _, err := w.Write(first.Data); err != nil {
			return 0, false, err
		}
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, false, err
		}
		if len(msg.Data) > 0 {
			if _, err := w.Write(msg.Data); err != nil {
				return 0, false, err
			}
		}
	}
	return version, true, nil
}

// PutStreamIfVersionLocal conditionally writes a streamed value to local storage
// without buffering it (issue #258): the reader is handed straight to the
// storage layer, which spills large values to a raw file. Not retried — a
// conditional write is not idempotent, and a stream is not replayable.
func (o *Operations) PutStreamIfVersionLocal(ctx context.Context, key string, r io.Reader, ttl int, expected uint64) (uint64, error) {
	return o.storage.PutIfVersion(key, r, ttl, expected)
}

// PutIfVersion writes key only if its current version equals expected (0 =
// put-if-absent). Returns the new version on success, or a
// *storageErrors.VersionMismatchError carrying the current version on a lost
// race.
func (o *Operations) PutIfVersion(ctx context.Context, key string, data []byte, ttl int, expected uint64) (uint64, error) {
	if o.IsLocal(key) {
		return o.storage.PutIfVersion(key, bytes.NewReader(data), ttl, expected)
	}
	return o.putIfVersionRemote(ctx, key, data, ttl, expected)
}

// DeleteIfVersion deletes key only if its current version equals expected.
func (o *Operations) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	if o.IsLocal(key) {
		return o.storage.DeleteIfVersion(key, expected)
	}
	return o.deleteIfVersionRemote(ctx, key, expected)
}

func (o *Operations) getWithVersionRemote(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for GetWithVersion")
		return nil, 0, false, err
	}
	client, err := o.Route(key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key for GetWithVersion")
		return nil, 0, false, err
	}
	resp, err := client.GetObjectWithVersion(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, 0, false, err
	}
	if !resp.Found {
		return nil, resp.Version, false, nil
	}
	return resp.Data, resp.Version, true, nil
}

func (o *Operations) putIfVersionRemote(ctx context.Context, key string, data []byte, ttl int, expected uint64) (uint64, error) {
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for PutIfVersion")
		return 0, err
	}
	client, err := o.Route(key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key for PutIfVersion")
		return 0, err
	}
	resp, err := client.PutObjectIfVersion(ctx, &pb.PutIfVersionRequest{
		Key:             key,
		TtlSeconds:      int64(ttl),
		Data:            data,
		ExpectedVersion: expected,
	})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, &operationError{message: resp.Error}
	}
	if !resp.Success {
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, resp.CurrentVersion)
	}
	return resp.NewVersion, nil
}

func (o *Operations) deleteIfVersionRemote(ctx context.Context, key string, expected uint64) error {
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for DeleteIfVersion")
		return err
	}
	client, err := o.Route(key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key for DeleteIfVersion")
		return err
	}
	resp, err := client.DeleteIfVersion(ctx, &pb.DeleteIfVersionRequest{Key: key, ExpectedVersion: expected})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return &operationError{message: resp.Error}
	}
	if !resp.Success {
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, resp.CurrentVersion)
	}
	return nil
}

// operationError is a plain remote-operation error (a real failure reported in
// a response's error field, distinct from a version mismatch).
type operationError struct{ message string }

func (e *operationError) Error() string { return e.message }
