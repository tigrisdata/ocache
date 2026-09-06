// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"io"

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
		if !found {
			return nil, version, false, nil
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
