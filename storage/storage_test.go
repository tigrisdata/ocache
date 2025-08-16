package storage

import (
	"bytes"
	"io"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
)

func TestStorage_PutGetDelete_SmallObject(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()
	key := "testkey"
	value := []byte("hello world")
	err := s.Put(key, bytes.NewReader(value), 0)
	assert.NoError(t, err, "Put failed")

	r, found, err := s.Get(key)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")
	if closer, ok := r.(io.Closer); ok {
		closer.Close() // Must close reader to release file lock before delete
	}

	s.DeleteKey(key)
	_, found, err = s.Get(key)
	assert.NoError(t, err, "Get after delete failed")
	assert.False(t, found, "expected key to be deleted")
}

func TestStorage_PutGetDelete_LargeObject(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024) // very low threshold to force spill
	defer cleanup()
	key := "largekey"
	value := []byte("this is a large value that should spill to disk")
	err := s.Put(key, bytes.NewReader(value), 0)
	assert.NoError(t, err, "Put failed")

	r, found, err := s.Get(key)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")
	if closer, ok := r.(io.Closer); ok {
		closer.Close() // Must close reader to release file lock before delete
	}

	s.DeleteKey(key)
	_, found, err = s.Get(key)
	assert.NoError(t, err, "Get after delete failed")
	assert.False(t, found, "expected key to be deleted")
}

func TestStorage_ListKeys(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		err := s.Put(k, bytes.NewReader([]byte(k)), 0)
		assert.NoError(t, err, "Put failed")
	}
	foundKeys, err := s.ListKeys()
	assert.NoError(t, err, "ListKeys failed")
	for _, k := range keys {
		assert.Contains(t, foundKeys, k, "key %q not found in ListKeys", k)
	}
}

func TestStorage_PutGet_TTL(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()
	key := "ttlkey"
	value := []byte("with ttl")
	err := s.Put(key, bytes.NewReader(value), 1) // 1 second TTL
	assert.NoError(t, err, "Put failed")

	r, found, err := s.Get(key)
	assert.NoError(t, err, "Get failed (before expiry)")
	assert.True(t, found, "Get did not find key (before expiry)")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value (before expiry)")

	t.Log("Waiting for TTL to expire...")
	time.Sleep(2 * time.Second)

	_, found, err = s.Get(key)
	assert.NoError(t, err, "Get failed (after expiry)")
	assert.False(t, found, "expected key to be expired and deleted")
}

func TestStorage_ListKeys_WithInternalKeys(t *testing.T) {
	// Test that ListKeys only returns user keys and skips internal keys
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add user keys
	userKeys := []string{"user1", "user2", "user3"}
	for _, key := range userKeys {
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err, "Put failed for %s", key)
	}

	// Directly add some internal keys to verify they're filtered out
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	s.meta.Handle().Put(wo, []byte("!access/user1"), []byte("12345678"))
	s.meta.Handle().Put(wo, []byte("!compact/00000000000000000001|user1"), []byte("/path"))

	// ListKeys should only return user keys
	keys, err := s.ListKeys()
	assert.NoError(t, err, "ListKeys failed")
	assert.Equal(t, len(userKeys), len(keys), "should return only user keys")

	for _, expected := range userKeys {
		assert.Contains(t, keys, expected, "should contain user key %s", expected)
	}
}

func TestStorage_ListKeys_WithExpiredKeys(t *testing.T) {
	// Test that ListKeys skips expired keys
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add permanent keys
	permanentKeys := []string{"perm1", "perm2"}
	for _, key := range permanentKeys {
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err, "Put failed for %s", key)
	}

	// Add expiring keys
	err := s.Put("expired", bytes.NewReader([]byte("value")), 1) // 1 second TTL
	assert.NoError(t, err, "Put failed for expired key")

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// ListKeys should only return non-expired keys
	keys, err := s.ListKeys()
	assert.NoError(t, err, "ListKeys failed")
	assert.Equal(t, len(permanentKeys), len(keys), "should return only non-expired keys")

	for _, expected := range permanentKeys {
		assert.Contains(t, keys, expected, "should contain permanent key %s", expected)
	}
	assert.NotContains(t, keys, "expired", "should not contain expired key")
}
