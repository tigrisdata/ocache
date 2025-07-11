package storage

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStorage_PutGetDelete_SmallObject(t *testing.T) {
	dir := t.TempDir()
	s, err := newStorage(dir, 3600, 1024, 4096, 16*1024*1024, 1000)
	assert.NoError(t, err, "failed to create storage")
	key := "testkey"
	value := []byte("hello world")
	err = s.Put(key, bytes.NewReader(value), 0)
	assert.NoError(t, err, "Put failed")

	r, found, err := s.Get(key)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")

	s.DeleteKey(key)
	_, found, err = s.Get(key)
	assert.NoError(t, err, "Get after delete failed")
	assert.False(t, found, "expected key to be deleted")
}

func TestStorage_PutGetDelete_LargeObject(t *testing.T) {
	dir := t.TempDir()
	s, err := newStorage(dir, 3600, 8, 4096, 16*1024*1024, 1000) // very low threshold to force spill
	assert.NoError(t, err, "failed to create storage")
	key := "largekey"
	value := []byte("this is a large value that should spill to disk")
	err = s.Put(key, bytes.NewReader(value), 0)
	assert.NoError(t, err, "Put failed")

	r, found, err := s.Get(key)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")

	s.DeleteKey(key)
	_, found, err = s.Get(key)
	assert.NoError(t, err, "Get after delete failed")
	assert.False(t, found, "expected key to be deleted")
}

func TestStorage_ListKeys(t *testing.T) {
	dir := t.TempDir()
	s, err := newStorage(dir, 3600, 1024, 4096, 16*1024*1024, 1000)
	assert.NoError(t, err, "failed to create storage")
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
	dir := t.TempDir()
	s, err := newStorage(dir, 3600, 1024, 4096, 16*1024*1024, 1000)
	assert.NoError(t, err, "failed to create storage")
	key := "ttlkey"
	value := []byte("with ttl")
	err = s.Put(key, bytes.NewReader(value), 1) // 1 second TTL
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
