// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/tigrisdata/ocache/proto"
)

// VersionMismatchError reports a lost conditional (CAS) operation: the key's
// current version did not equal the caller's expected version (issue #254).
// CurrentVersion carries the version the server observed, so the caller can
// re-read, re-decide, and retry with a fresh precondition. It is a normal
// outcome, not a transport failure.
type VersionMismatchError struct {
	Key            string
	CurrentVersion uint64
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("CAS version mismatch for %q (current version %d)", e.Key, e.CurrentVersion)
}

// IsVersionMismatch reports whether err is (or wraps) a VersionMismatchError,
// returning it for access to CurrentVersion.
func IsVersionMismatch(err error) (*VersionMismatchError, bool) {
	var vm *VersionMismatchError
	if errors.As(err, &vm) {
		return vm, true
	}
	return nil, false
}

// GetWithVersion returns a key's value together with its current CAS version.
// found is false (version 0) when the key is absent, expired, or deleted.
func (o *Operations) GetWithVersion(ctx context.Context, key string) (data []byte, version uint64, found bool, err error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return nil, 0, false, err
	}
	client := conn.getClient()
	if client == nil {
		return nil, 0, false, fmt.Errorf("no healthy connections available")
	}
	resp, err := client.GetObjectWithVersion(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, 0, false, err
	}
	return resp.Data, resp.Version, resp.Found, nil
}

// PutIfVersion writes the value only if the key's current version equals
// expected (0 = put-if-absent). It returns the new version on success, or a
// *VersionMismatchError carrying the current version on a lost race.
func (o *Operations) PutIfVersion(ctx context.Context, key string, data []byte, ttlSeconds int64, expected uint64) (uint64, error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return 0, err
	}
	client := conn.getClient()
	if client == nil {
		return 0, fmt.Errorf("no healthy connections available")
	}
	resp, err := client.PutObjectIfVersion(ctx, &pb.PutIfVersionRequest{
		Key:             key,
		TtlSeconds:      ttlSeconds,
		Data:            data,
		ExpectedVersion: expected,
	})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}
	if !resp.Success {
		return 0, &VersionMismatchError{Key: key, CurrentVersion: resp.CurrentVersion}
	}
	return resp.NewVersion, nil
}

// DeleteIfVersion deletes the key only if its current version equals expected.
// A lost race returns a *VersionMismatchError.
func (o *Operations) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}
	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}
	resp, err := client.DeleteIfVersion(ctx, &pb.DeleteIfVersionRequest{Key: key, ExpectedVersion: expected})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	if !resp.Success {
		return &VersionMismatchError{Key: key, CurrentVersion: resp.CurrentVersion}
	}
	return nil
}
