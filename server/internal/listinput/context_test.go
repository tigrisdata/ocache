// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package listinput

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestEmptyStartKeyCursorCapability(t *testing.T) {
	if HasEmptyStartKeyCursorCapability(nil) {
		t.Fatal("missing capability header must not advertise support")
	}
	if !HasEmptyStartKeyCursorCapability(metadata.Pairs(emptyStartKeyCursorCapabilityMetadata, emptyStartKeyCursorCapabilityVersion)) {
		t.Fatal("recognized capability header was not accepted")
	}
	if HasEmptyStartKeyCursorCapability(metadata.Pairs(emptyStartKeyCursorCapabilityMetadata, "other")) {
		t.Fatal("unknown capability version must not be accepted")
	}
}

func TestEmptyStartKeyCursorMetadataReplacesMarker(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"x-existing", "preserved",
		emptyStartKeyCursorMetadata, "stale",
		emptyStartKeyCursorMetadata, "true",
	))

	ctx = WithEmptyStartKeyCursor(ctx, false)
	outgoing, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("outgoing metadata was lost")
	}
	if got := outgoing.Get("x-existing"); len(got) != 1 || got[0] != "preserved" {
		t.Fatalf("unrelated metadata = %v, want preserved value", got)
	}
	if got := outgoing.Get(emptyStartKeyCursorMetadata); len(got) != 0 {
		t.Fatalf("empty-cursor marker = %v, want it removed", got)
	}
	if HasEmptyStartKeyCursor(ctx) {
		t.Fatal("outgoing marker must not be mistaken for incoming metadata")
	}

	ctx = WithEmptyStartKeyCursor(ctx, true)
	outgoing, ok = metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("outgoing metadata was lost after setting the marker")
	}
	if got := outgoing.Get(emptyStartKeyCursorMetadata); len(got) != 1 || got[0] != "true" {
		t.Fatalf("empty-cursor marker = %v, want one true value", got)
	}
	if !HasEmptyStartKeyCursor(metadata.NewIncomingContext(context.Background(), outgoing)) {
		t.Fatal("incoming empty-cursor marker was not recognized")
	}
}
