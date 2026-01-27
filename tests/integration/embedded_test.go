package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/embedded"
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
