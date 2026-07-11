// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"context"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
)

// Delete removes a key with automatic routing.
// If the key is local, it deletes from local storage.
// If the key is remote, it sends delete request via gRPC to the appropriate node.
func (o *Operations) Delete(ctx context.Context, key string) error {
	done := recordOperationStart("Operations.Delete")

	zlog.Debug().Str("key", key).Msg("Operations.Delete called")

	var err error
	if o.IsLocal(key) {
		err = o.DeleteLocal(ctx, key)
	} else {
		// Remote key - send via gRPC
		err = o.deleteRemote(ctx, key)
	}

	done(err)
	return err
}

// DeleteLocal deletes a key from local storage directly.
func (o *Operations) DeleteLocal(ctx context.Context, key string) error {
	return retry.DoWithKey(ctx, retry.DefaultConfig(), "Delete", key, func() error {
		return o.storage.DeleteKey(key)
	})
}

// deleteRemote sends a delete request to a remote node via gRPC.
func (o *Operations) deleteRemote(ctx context.Context, key string) error {
	// Increment hop count for forwarding loop detection
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for delete")
		return err
	}

	client, err := o.Route(key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key for delete")
		return err
	}

	req := &pb.DeleteRequest{Key: key}
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return &DeleteError{Message: resp.Error}
	}
	return nil
}

// DeleteError represents an error from a Delete operation.
type DeleteError struct {
	Message string
}

func (e *DeleteError) Error() string {
	return e.Message
}
