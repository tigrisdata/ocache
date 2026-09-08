// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/embedded"
	stor "github.com/tigrisdata/ocache/storage"
)

func TestEmbeddedClient_BasicOperations(t *testing.T) {
	// Create embedded client (single-node mode)
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Put
	err = client.Put(ctx, "key1", []byte("value1"), 0)
	require.NoError(t, err)

	// Get
	data, err := client.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), data)

	// List
	keys, err := client.List(ctx, "")
	require.NoError(t, err)
	assert.Contains(t, keys, "key1")

	// Delete
	err = client.Delete(ctx, "key1")
	require.NoError(t, err)

	// Verify deleted
	data, err = client.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestEmbeddedClient_StreamingOperations(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// PutStream
	data := []byte("streaming data for testing")
	err = client.PutStream(ctx, "stream-key", bytes.NewReader(data), 0)
	require.NoError(t, err)

	// GetStream
	var buf bytes.Buffer
	err = client.GetStream(ctx, "stream-key", &buf)
	require.NoError(t, err)
	assert.Equal(t, data, buf.Bytes())

	// GetRange
	rangeData, err := client.GetRange(ctx, "stream-key", 0, 9)
	require.NoError(t, err)
	assert.Equal(t, []byte("streaming "), rangeData)

	// GetRangeStream
	var rangeBuf bytes.Buffer
	err = client.GetRangeStream(ctx, "stream-key", 10, 14, &rangeBuf)
	require.NoError(t, err)
	assert.Equal(t, []byte("data "), rangeBuf.Bytes())
}

func TestEmbeddedClient_ListWithPrefix(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Create keys with different prefixes
	testKeys := []string{
		"user:alice",
		"user:bob",
		"session:123",
		"session:456",
		"cache:temp",
	}
	for _, key := range testKeys {
		err = client.Put(ctx, key, []byte(key), 0)
		require.NoError(t, err)
	}

	// List with prefix
	userKeys, err := client.List(ctx, "user:")
	require.NoError(t, err)
	assert.Len(t, userKeys, 2)
	assert.Contains(t, userKeys, "user:alice")
	assert.Contains(t, userKeys, "user:bob")

	sessionKeys, err := client.List(ctx, "session:")
	require.NoError(t, err)
	assert.Len(t, sessionKeys, 2)

	// List all
	allKeys, err := client.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, allKeys, 5)
}

func TestEmbeddedClient_ListPage(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Create multiple keys
	for i := 0; i < 10; i++ {
		key := string(rune('a'+i)) + "-key"
		err = client.Put(ctx, key, []byte(key), 0)
		require.NoError(t, err)
	}

	// First page
	keys, token, hasMore, err := client.ListPage(ctx, "", 3, "")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.True(t, hasMore)
	assert.NotEmpty(t, token)

	// Second page
	keys2, token2, hasMore2, err := client.ListPage(ctx, "", 3, token)
	require.NoError(t, err)
	assert.Len(t, keys2, 3)
	assert.True(t, hasMore2)
	assert.NotEmpty(t, token2)

	// Verify no overlap
	for _, k := range keys {
		assert.NotContains(t, keys2, k)
	}
}

func TestEmbeddedClient_ListPageWithValues(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Create multiple keys with distinct values
	for i := 0; i < 10; i++ {
		key := string(rune('a'+i)) + "-key"
		err = client.Put(ctx, key, []byte("value-"+key), 0)
		require.NoError(t, err)
	}

	// First page
	entries, token, hasMore, err := client.ListPageWithValues(ctx, "", 3, "")
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.True(t, hasMore)
	assert.NotEmpty(t, token)

	// Verify keys are sorted and values are correct
	for i := 1; i < len(entries); i++ {
		assert.Less(t, entries[i-1].Key, entries[i].Key)
	}
	for _, e := range entries {
		assert.Equal(t, []byte("value-"+e.Key), e.Value)
	}

	// Second page
	entries2, token2, hasMore2, err := client.ListPageWithValues(ctx, "", 3, token)
	require.NoError(t, err)
	assert.Len(t, entries2, 3)
	assert.True(t, hasMore2)
	assert.NotEmpty(t, token2)

	// Verify no overlap with first page
	firstKeys := make(map[string]bool)
	for _, e := range entries {
		firstKeys[e.Key] = true
	}
	for _, e := range entries2 {
		assert.False(t, firstKeys[e.Key])
		assert.Equal(t, []byte("value-"+e.Key), e.Value)
	}

	// Full iteration collects all entries with correct values
	var all []struct {
		key   string
		value []byte
	}
	tok := ""
	for {
		page, nextTok, more, err := client.ListPageWithValues(ctx, "", 4, tok)
		require.NoError(t, err)
		for _, e := range page {
			all = append(all, struct {
				key   string
				value []byte
			}{e.Key, e.Value})
		}
		if !more {
			break
		}
		tok = nextTok
	}
	assert.Len(t, all, 10)
	for _, e := range all {
		assert.Equal(t, []byte("value-"+e.key), e.value)
	}

	// Prefix filter
	entries, _, _, err = client.ListPageWithValues(ctx, "a-", 10, "")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "a-key", entries[0].Key)
	assert.Equal(t, []byte("value-a-key"), entries[0].Value)
}

func TestEmbeddedClient_ConfigValidation(t *testing.T) {
	// Missing DiskPath
	_, err := embedded.New(&embedded.Config{
		TTL: time.Hour,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DiskPath")

	// Missing TTL
	_, err = embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TTL")

	// Nil config
	_, err = embedded.New(nil)
	assert.Error(t, err)
}

func TestEmbeddedClient_GetMode(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	// Single-node mode should return ModeSimple
	mode := client.GetMode()
	assert.NotEmpty(t, mode)
}

func TestEmbeddedClient_GetConnectedNodes(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	nodes := client.GetConnectedNodes()
	// Single-node mode returns slice with empty node ID
	assert.NotNil(t, nodes)
}

func TestEmbeddedClient_IsLocal(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	// Single-node (non-cluster) mode: every key is owned locally, regardless
	// of the key. This is the branch consumers rely on to report local vs
	// remote serve locality.
	assert.True(t, client.IsLocal("any-key"))
	assert.True(t, client.IsLocal("body|bucket|object|etag"))
	assert.True(t, client.IsLocal(""))
}

// TestEmbeddedClient_AdvancedConfig exercises the Storage and Registerer
// plumbing to confirm advanced options flow through without breaking the
// client end-to-end. The specific tuning values here are chosen to be safe
// for a short-lived single-node cache.
func TestEmbeddedClient_AdvancedConfig(t *testing.T) {
	reg := prometheus.NewRegistry()

	client, err := embedded.New(&embedded.Config{
		DiskPath:   t.TempDir(),
		TTL:        time.Hour,
		Registerer: reg,
		Storage: &stor.StorageConfig{
			CompactionThreads: 2,
			SegmentSize:       16 << 20,
			FdCacheSize:       128,
			MetadataCacheSize: 32 << 20,
			CleanupInterval:   5 * time.Minute,
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	require.NoError(t, client.Put(ctx, "advanced", []byte("ok"), 0))

	data, err := client.Get(ctx, "advanced")
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), data)
}

func TestEmbeddedClient_IsReady(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	// Should be ready immediately in single-node mode
	assert.True(t, client.IsReady())
}

// TestEmbeddedClient_CASOperations exercises the conditional operations through
// the embedded client end to end in-process (issue #254/#258): the unary and
// streaming CAS paths through server/operations onto real storage, plus the
// embedded error translation that makes a lost race detectable with
// cacheclient.IsVersionMismatch (the gap the adversarial review found).
func TestEmbeddedClient_CASOperations(t *testing.T) {
	client, err := embedded.New(&embedded.Config{
		DiskPath: t.TempDir(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// --- unary CAS ---
	v1, err := client.PutIfVersion(ctx, "cas", []byte("v1"), 0, 0)
	require.NoError(t, err)
	require.NotZero(t, v1)

	data, ver, found, err := client.GetWithVersion(ctx, "cas")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, v1, ver)
	assert.Equal(t, []byte("v1"), data)

	// A lost race must be a *cacheclient.VersionMismatchError even via embedded.
	_, err = client.PutIfVersion(ctx, "cas", []byte("bad"), 0, v1+999)
	vm, ok := cacheclient.IsVersionMismatch(err)
	require.True(t, ok, "embedded must surface a detectable mismatch, got %v", err)
	assert.Equal(t, v1, vm.CurrentVersion)

	v2, err := client.PutIfVersion(ctx, "cas", []byte("v2"), 0, v1)
	require.NoError(t, err)
	assert.Greater(t, v2, v1)

	require.NoError(t, client.DeleteIfVersion(ctx, "cas", v2))
	_, _, found, err = client.GetWithVersion(ctx, "cas")
	require.NoError(t, err)
	assert.False(t, found)

	// --- streaming CAS (3MB value -> raw file, well past the unary path) ---
	big := bytes.Repeat([]byte("z"), 3*1024*1024)
	sv, err := client.PutStreamIfVersion(ctx, "big", bytes.NewReader(big), 0, 0)
	require.NoError(t, err)
	require.NotZero(t, sv)

	var buf bytes.Buffer
	gver, gfound, err := client.GetStreamWithVersion(ctx, "big", &buf)
	require.NoError(t, err)
	require.True(t, gfound)
	assert.Equal(t, sv, gver)
	assert.True(t, bytes.Equal(big, buf.Bytes()), "streamed value round-trip mismatch")

	// Guarded streaming update applies; a stale token loses with a detectable mismatch.
	sv2, err := client.PutStreamIfVersion(ctx, "big", bytes.NewReader(big), 0, sv)
	require.NoError(t, err)
	assert.Greater(t, sv2, sv)
	_, err = client.PutStreamIfVersion(ctx, "big", bytes.NewReader(big), 0, sv)
	_, ok = cacheclient.IsVersionMismatch(err)
	require.True(t, ok, "stale streaming token must mismatch, got %v", err)

	// Streaming get of an absent key reports found=false with an observation
	// token (#267), never 0.
	var gone bytes.Buffer
	gv, gf, err := client.GetStreamWithVersion(ctx, "absent", &gone)
	require.NoError(t, err)
	assert.False(t, gf)
	assert.NotZero(t, gv)
	assert.Zero(t, gone.Len())
}
