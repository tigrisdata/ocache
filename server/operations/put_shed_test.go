// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	stor "github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newShedTestStorage(t *testing.T) *stor.Storage {
	s, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:        t.TempDir(),
		TTL:             3600,
		InlineThreshold: 512,
		MaxDiskUsage:    4096,
		ShedDiskRatio:   1.0,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// A sheddable local put is refused only once the store is over its ratio, before
// the body is read; plain puts and the metadata the caller relies on still land.
func TestPutLocal_ShedOverBudget(t *testing.T) {
	s := newShedTestStorage(t)
	ops := New(s, nil)
	ctx := context.Background()

	sheddable := PutOptions{ShedOverBudget: true}
	require.NoError(t, ops.PutLocalWithOptions(ctx, "under", bytes.NewReader(make([]byte, 1024)), 0, sheddable))

	// Push over the cap with a plain write.
	require.NoError(t, ops.PutLocal(ctx, "fill", bytes.NewReader(make([]byte, 8192)), 0))
	require.True(t, s.OverShedRatio())

	body := bytes.NewReader(make([]byte, 1024))
	err := ops.PutLocalWithOptions(ctx, "over", body, 0, sheddable)
	require.Error(t, err)
	require.True(t, IsOverBudget(err), "want over-budget error, got %v", err)
	require.Equal(t, 1024, body.Len(), "a refused write must not consume the body")

	// Not sheddable: the store takes it (eviction is the cleaner's job).
	require.NoError(t, ops.PutLocal(ctx, "meta", bytes.NewReader([]byte("marker")), 0))
}

// The remote relay carries the owner's code and the explicit shed marker; the
// code alone (a genuine storage-full also maps to ResourceExhausted) is not a
// refusal.
func TestPutError_CarriesCodeAndShed(t *testing.T) {
	shed := error(&PutError{Message: "storage capacity exceeded", Code: codes.ResourceExhausted, Shed: true})
	require.Equal(t, codes.ResourceExhausted, status.Code(shed))
	require.True(t, IsOverBudget(shed))

	full := error(&PutError{Message: "storage capacity exceeded", Code: codes.ResourceExhausted})
	require.Equal(t, codes.ResourceExhausted, status.Code(full))
	require.False(t, IsOverBudget(full), "a real storage-full failure is not a shed")

	legacy := error(&PutError{Message: "boom"})
	require.Equal(t, codes.Unknown, status.Code(legacy))
	require.False(t, IsOverBudget(legacy))
	require.False(t, IsOverBudget(nil))
}
