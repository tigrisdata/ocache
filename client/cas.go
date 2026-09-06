// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	pb "github.com/tigrisdata/ocache/proto"
)

// casStreamChunk is the chunk size for streaming CAS values (issue #258).
const casStreamChunk = 1 << 20 // 1 MiB

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

// PutStreamIfVersion streams a value and applies it only if the key's current
// version equals expected (0 = put-if-absent) — the streaming form of
// PutIfVersion for values larger than the unary message cap (issue #258).
func (o *Operations) PutStreamIfVersion(ctx context.Context, key string, r io.Reader, ttlSeconds int64, expected uint64) (uint64, error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return 0, err
	}
	client := conn.getClient()
	if client == nil {
		return 0, fmt.Errorf("no healthy connections available")
	}
	stream, err := client.PutStreamIfVersion(ctx)
	if err != nil {
		return 0, err
	}
	// First message carries key/ttl/expected; subsequent messages carry data.
	if err := stream.Send(&pb.PutIfVersionRequest{Key: key, TtlSeconds: ttlSeconds, ExpectedVersion: expected}); err != nil {
		return 0, err
	}
	buf := make([]byte, casStreamChunk)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.PutIfVersionRequest{Data: buf[:n]}); err != nil {
				return 0, err
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, rerr
		}
	}
	resp, err := stream.CloseAndRecv()
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

// GetStreamWithVersion streams a key's value to w and returns its CAS version
// and presence, without buffering the whole value (issue #258). The version and
// data are one atomic snapshot: the server sends version+found first, then the
// bytes.
func (o *Operations) GetStreamWithVersion(ctx context.Context, key string, w io.Writer) (uint64, bool, error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return 0, false, err
	}
	client := conn.getClient()
	if client == nil {
		return 0, false, fmt.Errorf("no healthy connections available")
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
