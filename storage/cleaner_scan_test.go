// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"testing"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/metadata"
)

// seedForeignRows writes rows in keyspaces that sort on either side of the
// metadata prefix, standing in for the eviction indexes, back-references and
// queues that share the database with the metadata rows.
func seedForeignRows(t *testing.T, s *Storage, n int) {
	t.Helper()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := 0; i < n; i++ {
		require.NoError(t, s.meta.Handle().Put(wo, []byte(fmt.Sprintf("!aaa/%06d", i)), []byte("x")))
		require.NoError(t, s.meta.Handle().Put(wo, []byte(fmt.Sprintf("!zzz/%06d", i)), []byte("x")))
	}
}

func countRows(t *testing.T, s *Storage, prefix string) int {
	t.Helper()
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()
	p := []byte(prefix)
	n := 0
	for it.Seek(p); it.ValidForPrefix(p); it.Next() {
		n++
	}
	return n
}

// TestCleanerScans_WalkOnlyMetadataRows: the TTL sweep and the size
// reconciliation read exactly the metadata rows, however many rows the other
// keyspaces hold around them, and leave those rows alone.
func TestCleanerScans_WalkOnlyMetadataRows(t *testing.T) {
	s := newExpiryTestStorage(t)
	seedForeignRows(t, s, 500)
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Put(fmt.Sprintf("live-%d", i), bytes.NewReader([]byte("v")), 0))
	}
	writeExpiredInline(t, s, "expired-a", "a")
	writeExpiredInline(t, s, "expired-b", "b")

	ttl := metrics.CleanerRowsScanned.WithLabelValues("ttl")
	before := testutil.ToFloat64(ttl)
	s.cleaner.cleanupExpiredKeys()
	assert.Equal(t, 5.0, testutil.ToFloat64(ttl)-before, "the TTL sweep walks the metadata rows and nothing else")
	_, found := readValue(t, s, "expired-a")
	assert.False(t, found, "expired row deleted")
	v, found := readValue(t, s, "live-0")
	assert.True(t, found)
	assert.Equal(t, "v", v)

	reconcile := metrics.CleanerRowsScanned.WithLabelValues("reconcile")
	before = testutil.ToFloat64(reconcile)
	s.cleaner.reconcileFromMetadata()
	assert.Equal(t, 3.0, testutil.ToFloat64(reconcile)-before, "reconcile walks the metadata rows and nothing else")

	assert.Equal(t, 500, countRows(t, s, "!aaa/"))
	assert.Equal(t, 500, countRows(t, s, "!zzz/"))
}
