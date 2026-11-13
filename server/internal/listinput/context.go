// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package listinput preserves the source and presence of a list cursor across
// the server's service and operations packages.
package listinput

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	emptyStartKeyCursorMetadata           = "x-ocache-empty-start-key-cursor"
	emptyStartKeyCursorCapabilityMetadata = "x-ocache-empty-start-key-cursor-capability"
	emptyStartKeyCursorCapabilityVersion  = "v1"
)

type startKeyContextKey struct{}

// WithStartKey marks an explicitly present raw exclusive key. An empty key is
// meaningful when continuing after the empty key in a cluster page.
func WithStartKey(ctx context.Context, startKey string) context.Context {
	return context.WithValue(ctx, startKeyContextKey{}, startKey)
}

// StartKey returns the raw exclusive key carried by the request context.
func StartKey(ctx context.Context) (string, bool) {
	startKey, ok := ctx.Value(startKeyContextKey{}).(string)
	return startKey, ok
}

// WithEmptyStartKeyCursor replaces the outgoing node-local cursor marker so it
// cannot leak from an earlier shard call on the same parent context.
func WithEmptyStartKeyCursor(ctx context.Context, present bool) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	md.Delete(emptyStartKeyCursorMetadata)
	if present {
		md.Set(emptyStartKeyCursorMetadata, "true")
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// HasEmptyStartKeyCursor reports whether a node-local RPC carries an explicit
// empty exclusive cursor.
func HasEmptyStartKeyCursor(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, value := range md.Get(emptyStartKeyCursorMetadata) {
		if value == "true" {
			return true
		}
	}
	return false
}

// AdvertiseEmptyStartKeyCursorCapability reports that this node-local handler
// preserves explicit empty exclusive cursors. Older handlers omit the header.
func AdvertiseEmptyStartKeyCursorCapability(ctx context.Context) {
	_ = grpc.SendHeader(ctx, metadata.Pairs(emptyStartKeyCursorCapabilityMetadata, emptyStartKeyCursorCapabilityVersion))
}

// HasEmptyStartKeyCursorCapability reports whether a node-local response came
// from a handler that understands explicit empty exclusive cursors.
func HasEmptyStartKeyCursorCapability(md metadata.MD) bool {
	for _, value := range md.Get(emptyStartKeyCursorCapabilityMetadata) {
		if value == emptyStartKeyCursorCapabilityVersion {
			return true
		}
	}
	return false
}
