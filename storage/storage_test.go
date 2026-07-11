// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

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

	r, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")
	if closer, ok := r.(io.Closer); ok {
		closer.Close() // Must close reader to release file lock before delete
	}

	err = s.DeleteKey(key)
	assert.NoError(t, err, "DeleteKey failed")
	_, found, err = s.Get(key, 0, 0)
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

	r, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Get did not find key")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value")
	if closer, ok := r.(io.Closer); ok {
		closer.Close() // Must close reader to release file lock before delete
	}

	err = s.DeleteKey(key)
	assert.NoError(t, err, "DeleteKey failed")
	_, found, err = s.Get(key, 0, 0)
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
	foundKeys, err := s.ListKeys("")
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

	r, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err, "Get failed (before expiry)")
	assert.True(t, found, "Get did not find key (before expiry)")
	got, err := io.ReadAll(r)
	assert.NoError(t, err, "ReadAll failed")
	assert.Equal(t, value, got, "Get returned wrong value (before expiry)")

	t.Log("Waiting for TTL to expire...")
	time.Sleep(2 * time.Second)

	_, found, err = s.Get(key, 0, 0)
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
	keys, err := s.ListKeys("")
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
	keys, err := s.ListKeys("")
	assert.NoError(t, err, "ListKeys failed")
	assert.Equal(t, len(permanentKeys), len(keys), "should return only non-expired keys")

	for _, expected := range permanentKeys {
		assert.Contains(t, keys, expected, "should contain permanent key %s", expected)
	}
	assert.NotContains(t, keys, "expired", "should not contain expired key")
}

func TestStorage_ListKeys_WithPrefix(t *testing.T) {
	// Test that ListKeys correctly filters by prefix using RocksDB prefix iteration
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add keys with different prefixes
	testKeys := map[string]string{
		"user:alice":    "alice_data",
		"user:bob":      "bob_data",
		"user:charlie":  "charlie_data",
		"session:123":   "session_data",
		"session:456":   "session_data",
		"cache:temp":    "temp_data",
		"cache:persist": "persist_data",
		"noprefix":      "no_prefix_data",
	}

	// Put all test keys
	for key, value := range testKeys {
		err := s.Put(key, bytes.NewReader([]byte(value)), 0)
		assert.NoError(t, err, "Put failed for %s", key)
	}

	// Test different prefix scenarios
	testCases := []struct {
		name     string
		prefix   string
		expected []string
	}{
		{
			name:     "user prefix",
			prefix:   "user:",
			expected: []string{"user:alice", "user:bob", "user:charlie"},
		},
		{
			name:     "session prefix",
			prefix:   "session:",
			expected: []string{"session:123", "session:456"},
		},
		{
			name:     "cache prefix",
			prefix:   "cache:",
			expected: []string{"cache:temp", "cache:persist"},
		},
		{
			name:     "partial prefix user",
			prefix:   "user",
			expected: []string{"user:alice", "user:bob", "user:charlie"},
		},
		{
			name:     "single char prefix",
			prefix:   "u",
			expected: []string{"user:alice", "user:bob", "user:charlie"},
		},
		{
			name:     "non-existent prefix",
			prefix:   "notfound:",
			expected: []string{},
		},
		{
			name:     "empty prefix returns all",
			prefix:   "",
			expected: []string{"user:alice", "user:bob", "user:charlie", "session:123", "session:456", "cache:temp", "cache:persist", "noprefix"},
		},
		{
			name:     "specific key as prefix",
			prefix:   "user:alice",
			expected: []string{"user:alice"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keys, err := s.ListKeys(tc.prefix)
			assert.NoError(t, err, "ListKeys failed for prefix %q", tc.prefix)
			assert.ElementsMatch(t, tc.expected, keys, "unexpected keys for prefix %q", tc.prefix)
		})
	}
}

// TestStorage_Get_ByteRange_SmallObject tests byte-range requests for inline (small) objects
func TestStorage_Get_ByteRange_SmallObject(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "byterangekey"
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	// Store the data
	err := s.Put(key, bytes.NewReader(data), 0)
	assert.NoError(t, err, "Put failed")

	testCases := []struct {
		name     string
		start    int64
		end      int64
		expected string
	}{
		{
			name:     "Full read (end=0 for EOF)",
			start:    0,
			end:      0, // end <= 0 means read to EOF
			expected: "0123456789abcdefghijklmnopqrstuvwxyz",
		},
		{
			name:     "Read first 10 bytes",
			start:    0,
			end:      9, // inclusive: bytes 0-9
			expected: "0123456789",
		},
		{
			name:     "Read middle 10 bytes",
			start:    10,
			end:      19, // inclusive: bytes 10-19
			expected: "abcdefghij",
		},
		{
			name:     "Read last 10 bytes",
			start:    26,
			end:      35, // inclusive: bytes 26-35
			expected: "qrstuvwxyz",
		},
		{
			name:     "Read from offset to end",
			start:    20,
			end:      0, // end <= 0 means read to EOF
			expected: "klmnopqrstuvwxyz",
		},
		{
			name:     "Single byte in middle",
			start:    15,
			end:      15, // inclusive: byte 15 only
			expected: "f",
		},
		{
			name:     "Single byte at end",
			start:    35,
			end:      35, // inclusive: byte 35 only
			expected: "z",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r, found, err := s.Get(key, tc.start, tc.end)
			assert.NoError(t, err, "Get failed")
			assert.True(t, found, "Key not found")

			got, err := io.ReadAll(r)
			assert.NoError(t, err, "ReadAll failed")
			assert.Equal(t, tc.expected, string(got), "Byte range read returned wrong data")

			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
		})
	}
}

// TestStorage_Get_ByteRange_LargeObject tests byte-range requests for file-based (large) objects
func TestStorage_Get_ByteRange_LargeObject(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024) // low threshold to force file storage
	defer cleanup()

	key := "largebyterangekey"
	// Create a large repeating pattern for testing
	pattern := []byte("0123456789")
	data := bytes.Repeat(pattern, 100) // 1000 bytes total

	// Store the data
	err := s.Put(key, bytes.NewReader(data), 0)
	assert.NoError(t, err, "Put failed")

	testCases := []struct {
		name     string
		start    int64
		end      int64
		expected string
	}{
		{
			name:     "Read first pattern",
			start:    0,
			end:      9, // inclusive: bytes 0-9
			expected: "0123456789",
		},
		{
			name:     "Read across pattern boundary",
			start:    5,
			end:      14, // inclusive: bytes 5-14
			expected: "5678901234",
		},
		{
			name:     "Read middle section",
			start:    500,
			end:      509, // inclusive: bytes 500-509
			expected: "0123456789",
		},
		{
			name:     "Read last 10 bytes",
			start:    990,
			end:      999, // inclusive: bytes 990-999
			expected: "0123456789",
		},
		{
			name:     "Read from offset to end",
			start:    995,
			end:      0, // end <= 0 means read to EOF
			expected: "56789",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r, found, err := s.Get(key, tc.start, tc.end)
			assert.NoError(t, err, "Get failed")
			assert.True(t, found, "Key not found")

			got, err := io.ReadAll(r)
			assert.NoError(t, err, "ReadAll failed")
			assert.Equal(t, tc.expected, string(got), "Byte range read returned wrong data")

			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
		})
	}
}

// TestStorage_Get_ByteRange_EdgeCases tests edge cases and error conditions
func TestStorage_Get_ByteRange_EdgeCases(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "edgekey"
	data := []byte("0123456789")

	// Store the data
	err := s.Put(key, bytes.NewReader(data), 0)
	assert.NoError(t, err, "Put failed")

	testCases := []struct {
		name     string
		start    int64
		end      int64
		expected string
		desc     string
	}{
		{
			name:     "Start beyond data length",
			start:    100,
			end:      0, // end <= 0 means read to EOF
			expected: "",
			desc:     "Should return empty when start is beyond data",
		},
		{
			name:     "End beyond data length",
			start:    5,
			end:      100, // inclusive, but clamped to 9
			expected: "56789",
			desc:     "Should read until actual end of data",
		},
		{
			name:     "Start and end beyond data",
			start:    100,
			end:      200,
			expected: "",
			desc:     "Should return empty when range is beyond data",
		},
		{
			name:     "End equals start",
			start:    5,
			end:      5, // inclusive: returns single byte at position 5
			expected: "5",
			desc:     "Should return single byte when end equals start (inclusive)",
		},
		{
			name:     "End less than start",
			start:    10,
			end:      5,
			expected: "",
			desc:     "Should return empty when end < start",
		},
		{
			name:     "Read to EOF with end=0",
			start:    0,
			end:      0, // end <= 0 means read to EOF
			expected: "0123456789",
			desc:     "Should return full data when end <= 0 (read to EOF)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r, found, err := s.Get(key, tc.start, tc.end)
			assert.NoError(t, err, "Get failed")
			assert.True(t, found, "Key not found")

			got, err := io.ReadAll(r)
			assert.NoError(t, err, "ReadAll failed")
			assert.Equal(t, tc.expected, string(got), tc.desc)

			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
		})
	}
}

// TestStorage_Get_ByteRange_MultipleReads tests multiple concurrent byte-range reads
func TestStorage_Get_ByteRange_MultipleReads(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "multireadkey"
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	// Store the data
	err := s.Put(key, bytes.NewReader(data), 0)
	assert.NoError(t, err, "Put failed")

	// Get multiple readers with different ranges (inclusive end)
	r1, found1, err1 := s.Get(key, 0, 9) // inclusive: bytes 0-9
	assert.NoError(t, err1, "First Get failed")
	assert.True(t, found1, "First Get: key not found")

	r2, found2, err2 := s.Get(key, 10, 19) // inclusive: bytes 10-19
	assert.NoError(t, err2, "Second Get failed")
	assert.True(t, found2, "Second Get: key not found")

	r3, found3, err3 := s.Get(key, 20, 0) // end <= 0 means read to EOF
	assert.NoError(t, err3, "Third Get failed")
	assert.True(t, found3, "Third Get: key not found")

	// Read from all readers
	got1, _ := io.ReadAll(r1)
	got2, _ := io.ReadAll(r2)
	got3, _ := io.ReadAll(r3)

	assert.Equal(t, "0123456789", string(got1), "First reader wrong data")
	assert.Equal(t, "abcdefghij", string(got2), "Second reader wrong data")
	assert.Equal(t, "klmnopqrstuvwxyz", string(got3), "Third reader wrong data")

	// Close all readers
	if closer, ok := r1.(io.Closer); ok {
		closer.Close()
	}
	if closer, ok := r2.(io.Closer); ok {
		closer.Close()
	}
	if closer, ok := r3.(io.Closer); ok {
		closer.Close()
	}
}

// TestStorage_Get_ByteRange_PartialReads tests reading in chunks with byte ranges
func TestStorage_Get_ByteRange_PartialReads(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	key := "partialkey"
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	// Store the data
	err := s.Put(key, bytes.NewReader(data), 0)
	assert.NoError(t, err, "Put failed")

	// Get a reader for a specific range (inclusive: bytes 10-29)
	r, found, err := s.Get(key, 10, 29)
	assert.NoError(t, err, "Get failed")
	assert.True(t, found, "Key not found")
	defer func() {
		if closer, ok := r.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Read in small chunks
	buf := make([]byte, 5)
	var result []byte

	for {
		n, err := r.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		assert.NoError(t, err, "Read failed")
	}

	assert.Equal(t, "abcdefghijklmnopqrst", string(result), "Partial reads returned wrong data")
}

func TestStorage_ListKeysWithPagination_Basic(t *testing.T) {
	// Test basic pagination with default limit
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Create 50 keys
	keyCount := 50
	for i := 0; i < keyCount; i++ {
		key := "key-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err, "Put failed for %s", key)
	}

	// Get first page with limit 10
	keys, lastKey, hasMore, err := s.ListKeysWithPagination("", "", 10)
	assert.NoError(t, err, "ListKeysWithPagination failed")
	assert.Len(t, keys, 10, "should return 10 keys")
	assert.True(t, hasMore, "should have more keys")
	assert.NotEmpty(t, lastKey, "lastKey should not be empty when hasMore=true")

	// Verify keys are sorted
	for i := 1; i < len(keys); i++ {
		assert.LessOrEqual(t, keys[i-1], keys[i], "keys should be in sorted order")
	}
}

func TestStorage_ListKeysWithPagination_WithPrefix(t *testing.T) {
	// Test pagination with prefix filtering
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Create keys with different prefixes
	prefixes := []string{"user-", "session-", "cache-"}
	for _, prefix := range prefixes {
		for i := 0; i < 15; i++ {
			key := prefix + string(rune('a'+i))
			err := s.Put(key, bytes.NewReader([]byte("value")), 0)
			assert.NoError(t, err, "Put failed for %s", key)
		}
	}

	// List only "user-" prefix with pagination
	keys, lastKey, hasMore, err := s.ListKeysWithPagination("user-", "", 10)
	assert.NoError(t, err, "ListKeysWithPagination failed")
	assert.Len(t, keys, 10, "should return 10 keys")
	assert.True(t, hasMore, "should have more user- keys")
	assert.NotEmpty(t, lastKey, "lastKey should not be empty")

	// Verify all keys have the prefix
	for _, key := range keys {
		assert.True(t, len(key) >= 5 && key[:5] == "user-", "all keys should have user- prefix")
	}

	// Get next page
	keys2, lastKey2, hasMore2, err := s.ListKeysWithPagination("user-", lastKey, 10)
	assert.NoError(t, err, "ListKeysWithPagination failed for page 2")
	assert.Len(t, keys2, 5, "should return remaining 5 keys")
	assert.False(t, hasMore2, "should not have more keys")
	assert.Empty(t, lastKey2, "lastKey should be empty when hasMore=false")

	// Verify no overlap between pages
	firstPageMap := make(map[string]bool)
	for _, k := range keys {
		firstPageMap[k] = true
	}
	for _, k := range keys2 {
		assert.False(t, firstPageMap[k], "pages should not overlap: key %s in both pages", k)
	}
}

func TestStorage_ListKeysWithPagination_ContinuationToken(t *testing.T) {
	// Test that continuation token (startKey) works correctly
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Create 30 keys
	for i := 0; i < 30; i++ {
		key := "item-" + string(rune('a'+i))
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err, "Put failed for %s", key)
	}

	// Get first page
	page1, lastKey1, hasMore1, err := s.ListKeysWithPagination("", "", 10)
	assert.NoError(t, err)
	assert.Len(t, page1, 10)
	assert.True(t, hasMore1)

	// Get second page using continuation token
	page2, lastKey2, hasMore2, err := s.ListKeysWithPagination("", lastKey1, 10)
	assert.NoError(t, err)
	assert.Len(t, page2, 10)
	assert.True(t, hasMore2)

	// Get third page
	page3, lastKey3, hasMore3, err := s.ListKeysWithPagination("", lastKey2, 10)
	assert.NoError(t, err)
	assert.Len(t, page3, 10)
	assert.False(t, hasMore3)
	assert.Empty(t, lastKey3)

	// Verify ordering continues across pages
	assert.Less(t, page1[len(page1)-1], page2[0], "page 2 should start after page 1")
	assert.Less(t, page2[len(page2)-1], page3[0], "page 3 should start after page 2")
}

func TestStorage_ListKeysWithPagination_EmptyResults(t *testing.T) {
	// Test pagination with no matching keys
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add keys with different prefix
	err := s.Put("foo-1", bytes.NewReader([]byte("value")), 0)
	assert.NoError(t, err)

	// Query with non-existent prefix
	keys, lastKey, hasMore, err := s.ListKeysWithPagination("bar-", "", 10)
	assert.NoError(t, err, "ListKeysWithPagination should not error on empty results")
	assert.Empty(t, keys, "should return empty keys")
	assert.False(t, hasMore, "should not have more")
	assert.Empty(t, lastKey, "lastKey should be empty")
}

func TestStorage_ListKeysWithPagination_SkipsExpiredKeys(t *testing.T) {
	// Test that pagination skips expired keys without including them in the count
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add permanent keys
	for i := 0; i < 5; i++ {
		key := "perm-" + string(rune('a'+i))
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err)
	}

	// Add expiring keys
	for i := 0; i < 5; i++ {
		key := "temp-" + string(rune('a'+i))
		err := s.Put(key, bytes.NewReader([]byte("value")), 1) // 1 second TTL
		assert.NoError(t, err)
	}

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// List all keys - should only return permanent keys
	keys, lastKey, hasMore, err := s.ListKeysWithPagination("", "", 100)
	assert.NoError(t, err)
	assert.Len(t, keys, 5, "should return only non-expired keys")
	assert.False(t, hasMore, "should not have more keys")
	assert.Empty(t, lastKey, "lastKey should be empty")

	// Verify no expired keys in results
	for _, key := range keys {
		assert.True(t, len(key) >= 5 && key[:5] == "perm-", "should only contain permanent keys")
	}
}

func TestStorage_ListKeysWithPagination_SkipsInternalKeys(t *testing.T) {
	// Test that pagination skips internal keys
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add user keys
	userKeys := []string{"user-a", "user-b", "user-c"}
	for _, key := range userKeys {
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err)
	}

	// Directly add internal keys
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	s.meta.Handle().Put(wo, []byte("!access/user-a"), []byte("12345678"))
	s.meta.Handle().Put(wo, []byte("!compact/00000000000000000001|user-b"), []byte("/path"))

	// List should only return user keys
	keys, _, _, err := s.ListKeysWithPagination("", "", 100)
	assert.NoError(t, err)
	assert.Len(t, keys, len(userKeys), "should return only user keys")

	for _, expected := range userKeys {
		assert.Contains(t, keys, expected, "should contain user key %s", expected)
	}
}

func TestStorage_ListKeysWithPagination_SortOrder(t *testing.T) {
	// Test that keys are returned in lexicographic order
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Add keys in random order
	unorderedKeys := []string{"zebra", "apple", "mango", "banana", "kiwi"}
	for _, key := range unorderedKeys {
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err)
	}

	// List all keys
	keys, _, _, err := s.ListKeysWithPagination("", "", 100)
	assert.NoError(t, err)
	assert.Len(t, keys, len(unorderedKeys))

	// Verify sorted order
	expectedOrder := []string{"apple", "banana", "kiwi", "mango", "zebra"}
	assert.Equal(t, expectedOrder, keys, "keys should be in lexicographic order")
}

func TestStorage_ListKeysWithPagination_MultiPage(t *testing.T) {
	// Test paginating through many keys
	s, cleanup := createTestStorageWithDefaults(t)
	defer cleanup()

	// Create 100 keys
	keyCount := 100
	expectedKeys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		key := "key-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		expectedKeys[i] = key
		err := s.Put(key, bytes.NewReader([]byte("value")), 0)
		assert.NoError(t, err)
	}

	// Paginate through all keys
	var allKeys []string
	continuationToken := ""
	pageCount := 0
	pageSize := 15

	for {
		keys, lastKey, hasMore, err := s.ListKeysWithPagination("", continuationToken, pageSize)
		assert.NoError(t, err)

		pageCount++
		allKeys = append(allKeys, keys...)

		if !hasMore {
			break
		}

		assert.NotEmpty(t, lastKey, "lastKey should not be empty when hasMore=true")
		continuationToken = lastKey
	}

	// Verify we got all keys
	assert.Len(t, allKeys, keyCount, "should receive all keys across pages")

	// Verify no duplicates
	keyMap := make(map[string]int)
	for _, key := range allKeys {
		keyMap[key]++
	}
	for key, count := range keyMap {
		assert.Equal(t, 1, count, "key %s should appear only once", key)
	}

	// Verify global sort order
	for i := 1; i < len(allKeys); i++ {
		assert.LessOrEqual(t, allKeys[i-1], allKeys[i], "keys should be in sorted order across pages")
	}

	t.Logf("Successfully paginated through %d keys in %d pages (page size %d)", keyCount, pageCount, pageSize)
}
