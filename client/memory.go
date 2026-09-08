// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MemoryCache implements CacheClient with in-memory storage.
// Useful for testing without a real cache server.
type MemoryCache struct {
	mu   sync.RWMutex
	data map[string]cacheEntry
	// versions and versionCounter back the conditional (CAS) operations. The
	// counter is a monotonic stamp source mirroring the real storage layer.
	versions       map[string]uint64
	versionCounter uint64
	// fences records, per CAS-deleted key, the stamp of the delete (issue
	// #267): an absent read hands it out as the key's observation token and a
	// put or delete carrying an older token loses to it. Mirrors the stamped
	// tombstone real storage retains; the double keeps fences until a CAS put
	// recreates the key, a plain op drops it, or Close.
	fences map[string]uint64
}

// cacheEntry holds a cached value with optional expiration.
type cacheEntry struct {
	value     []byte
	expiresAt time.Time // Zero value means no expiration
}

// Compile-time check that MemoryCache implements CacheClient.
var _ CacheClient = (*MemoryCache)(nil)

// memoryLegacyVersion mirrors the storage layer's merge.VersionLegacy: a live
// key written by a plain Put (no CAS-assigned stamp) reports this version, and
// CAS stamps are assigned strictly above it. Kept in sync by value so this test
// double behaves like real storage (a mismatch here matches production).
const memoryLegacyVersion uint64 = 1

// NewMemoryCache creates a new in-memory cache.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		data:     make(map[string]cacheEntry),
		versions: make(map[string]uint64),
		fences:   make(map[string]uint64),
		// Start above memoryLegacyVersion so the first CAS stamp (++counter) is 2
		// and can never collide with the legacy sentinel a plain Put reports.
		versionCounter: memoryLegacyVersion,
	}
}

// liveLocked reports whether key currently holds a live (unexpired) value.
// Caller must hold m.mu.
func (m *MemoryCache) liveLocked(key string) bool {
	entry, ok := m.data[key]
	if !ok {
		return false
	}
	return entry.expiresAt.IsZero() || time.Now().Before(entry.expiresAt)
}

// effectiveVersionLocked returns the CAS version for key, mirroring storage's
// EffectiveRowVersion: 0 when absent or expired; the legacy sentinel for a live
// key with no CAS-assigned stamp (a plain Put); else its stamp. Caller holds m.mu.
func (m *MemoryCache) effectiveVersionLocked(key string) uint64 {
	if !m.liveLocked(key) {
		return 0
	}
	if v := m.versions[key]; v != 0 {
		return v
	}
	return memoryLegacyVersion
}

// dropLocked removes key from the cache AND forgets its CAS stamp, so a later
// plain re-create reads as the legacy version rather than resurfacing a stale
// token. Used by plain Delete, lazy TTL expiry and Close. Caller holds m.mu.
func (m *MemoryCache) dropLocked(key string) {
	delete(m.data, key)
	delete(m.versions, key)
	delete(m.fences, key)
}

// absenceTokenLocked mirrors storage's absent-read token (issue #267): the
// fence stamp if the key was CAS-deleted, else a fresh stamp. Caller holds m.mu
// for writing.
func (m *MemoryCache) absenceTokenLocked(key string) uint64 {
	if f, ok := m.fences[key]; ok {
		return f
	}
	m.versionCounter++
	return m.versionCounter
}

// admitsLocked mirrors storage's casAdmits: a live key needs an exact match; a
// fenced key admits 0 or a token at or after the fence; absence admits
// anything. Returns the version a mismatch reports. Caller holds m.mu.
func (m *MemoryCache) admitsLocked(key string, expected uint64) (bool, uint64) {
	if m.liveLocked(key) {
		cur := m.effectiveVersionLocked(key)
		return cur == expected, cur
	}
	if f, ok := m.fences[key]; ok {
		return expected == 0 || expected >= f, f
	}
	return true, 0
}

// GetWithVersion returns key's value and version. For an absent or expired key
// found is false and the version is an observation token (issue #267): the
// fence stamp of the CAS delete that removed it, else a fresh stamp. Pass it
// back to PutIfVersion to order the write against any later delete.
func (m *MemoryCache) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, false, err
	}
	m.mu.Lock() // a fresh token advances the stamp counter
	defer m.mu.Unlock()
	if !m.liveLocked(key) {
		return nil, m.absenceTokenLocked(key), false, nil
	}
	entry := m.data[key]
	out := make([]byte, len(entry.value))
	copy(out, entry.value)
	return out, m.effectiveVersionLocked(key), true, nil
}

// PutIfVersion writes only if key admits expected: a live key needs an exact
// match; an absent key admits 0 (put-if-absent) or an observation token, which
// a CAS delete stamped after the observation rejects (issue #267). Returns the
// new version or a *VersionMismatchError carrying the current version (the
// fence stamp, for a fenced key).
func (m *MemoryCache) PutIfVersion(ctx context.Context, key string, data []byte, ttlSeconds int64, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok, cur := m.admitsLocked(key, expected); !ok {
		return 0, &VersionMismatchError{Key: key, CurrentVersion: cur}
	}
	delete(m.fences, key)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	entry := cacheEntry{value: dataCopy}
	if ttlSeconds > 0 {
		entry.expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	m.versionCounter++
	m.data[key] = entry
	m.versions[key] = m.versionCounter
	return m.versionCounter, nil
}

// DeleteIfVersion deletes only if key admits expected (see PutIfVersion), and
// records the delete as the key's fence (issue #267): delete-if-absent on a
// missing or dead key is not a no-op, it writes or moves the fence so a
// populate that observed absence before this delete loses to it.
func (m *MemoryCache) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.liveLocked(key):
		if cur := m.effectiveVersionLocked(key); cur != expected {
			return &VersionMismatchError{Key: key, CurrentVersion: cur}
		}
	default:
		if f, fenced := m.fences[key]; fenced {
			if !(expected == 0 || expected >= f) {
				return &VersionMismatchError{Key: key, CurrentVersion: f}
			}
		} else if expected != 0 {
			return &VersionMismatchError{Key: key, CurrentVersion: 0}
		}
	}
	delete(m.data, key)
	delete(m.versions, key)
	m.versionCounter++
	m.fences[key] = m.versionCounter
	return nil
}

// Put stores data with an optional TTL (0 means no expiration).
func (m *MemoryCache) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Copy data to avoid mutation issues
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	entry := cacheEntry{value: dataCopy}
	if ttlSeconds > 0 {
		entry.expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}

	m.data[key] = entry
	// A plain overwrite replaces any tombstone row in storage, so it drops the
	// fence here too (mixing plain and CAS ops on a key voids the guarantee).
	delete(m.fences, key)
	// A plain (non-CAS) write stores no stamp, exactly like storage: the row
	// reads as the legacy version from here on and any older CAS token must be
	// rejected, never silently accepted against a value it did not guard.
	delete(m.versions, key)
	return nil
}

// PutStream reads all data from the reader and stores it.
func (m *MemoryCache) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return m.Put(ctx, key, data, ttlSeconds)
}

// Get retrieves data by key.
func (m *MemoryCache) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	entry, exists := m.data[key]
	m.mu.RUnlock()

	if !exists {
		return nil, status.Error(codes.NotFound, "key not found")
	}

	// Check TTL expiration (lazy expiration)
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		m.mu.Lock()
		m.dropLocked(key)
		m.mu.Unlock()
		return nil, status.Error(codes.NotFound, "key not found")
	}

	// Return a copy to avoid mutation issues
	result := make([]byte, len(entry.value))
	copy(result, entry.value)
	return result, nil
}

// GetStream retrieves data and writes it to the writer.
func (m *MemoryCache) GetStream(ctx context.Context, key string, w io.Writer) error {
	data, err := m.Get(ctx, key)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// GetRange retrieves a byte range from the cached data.
func (m *MemoryCache) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	entry, exists := m.data[key]
	m.mu.RUnlock()

	if !exists {
		return nil, status.Error(codes.NotFound, "key not found")
	}

	// Check TTL expiration
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		m.mu.Lock()
		m.dropLocked(key)
		m.mu.Unlock()
		return nil, status.Error(codes.NotFound, "key not found")
	}

	dataLen := int64(len(entry.value))

	// Normalize range (inclusive end semantics; end <= 0 means read to EOF)
	if start < 0 {
		start = 0
	}
	// end <= 0 means "read to EOF", convert to last valid index
	if end <= 0 {
		end = dataLen - 1
	} else if end >= dataLen {
		end = dataLen - 1
	}
	// Validation: start > end is invalid (start == end returns 1 byte)
	if start >= dataLen || start > end {
		return nil, status.Error(codes.InvalidArgument, "invalid range")
	}

	// Return a copy (inclusive: end-start+1 bytes)
	result := make([]byte, end-start+1)
	copy(result, entry.value[start:end+1])
	return result, nil
}

// GetRangeStream retrieves a byte range and writes it to the writer.
func (m *MemoryCache) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	data, err := m.GetRange(ctx, key, start, end)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// Delete removes a key from the cache.
func (m *MemoryCache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[key]; !exists {
		return status.Error(codes.NotFound, "key not found")
	}

	m.dropLocked(key)
	return nil
}

// List returns all keys matching the prefix.
func (m *MemoryCache) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	now := time.Now()

	for key, entry := range m.data {
		// Skip expired entries
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			continue
		}

		if prefix == "" || strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys, nil
}

// ListPage returns a paginated list of keys matching the prefix.
func (m *MemoryCache) ListPage(ctx context.Context, prefix string, limit int, continuationToken string) (keys []string, nextToken string, hasMore bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}

	// Get all matching keys
	allKeys, err := m.List(ctx, prefix)
	if err != nil {
		return nil, "", false, err
	}

	// Default limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 1000 {
		limit = 1000
	}

	// Find starting position based on continuation token
	startIdx := 0
	if continuationToken != "" {
		for i, k := range allKeys {
			if k > continuationToken {
				startIdx = i
				break
			}
		}
		// If token is greater than all keys, start after the last key
		if startIdx == 0 && len(allKeys) > 0 && continuationToken >= allKeys[len(allKeys)-1] {
			return nil, "", false, nil
		}
	}

	// Calculate end position
	endIdx := startIdx + limit
	if endIdx > len(allKeys) {
		endIdx = len(allKeys)
	}

	keys = allKeys[startIdx:endIdx]
	hasMore = endIdx < len(allKeys)

	if hasMore && len(keys) > 0 {
		nextToken = keys[len(keys)-1]
	}

	return keys, nextToken, hasMore, nil
}

// ListPageWithValues returns a paginated list of key-value pairs matching the prefix.
func (m *MemoryCache) ListPageWithValues(ctx context.Context, prefix string, limit int, continuationToken string) (entries []KeyValue, nextToken string, hasMore bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Build sorted key list under a single lock to avoid TOCTOU between key scan and value reads
	var allKeys []string
	now := time.Now()
	for key, entry := range m.data {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			continue
		}
		if prefix == "" || strings.HasPrefix(key, prefix) {
			allKeys = append(allKeys, key)
		}
	}
	sort.Strings(allKeys)

	// Default limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 1000 {
		limit = 1000
	}

	// Find starting position based on continuation token
	startIdx := 0
	if continuationToken != "" {
		for i, k := range allKeys {
			if k > continuationToken {
				startIdx = i
				break
			}
		}
		if startIdx == 0 && len(allKeys) > 0 && continuationToken >= allKeys[len(allKeys)-1] {
			return nil, "", false, nil
		}
	}

	endIdx := startIdx + limit
	if endIdx > len(allKeys) {
		endIdx = len(allKeys)
	}

	pageKeys := allKeys[startIdx:endIdx]
	hasMore = endIdx < len(allKeys)

	if hasMore && len(pageKeys) > 0 {
		nextToken = pageKeys[len(pageKeys)-1]
	}

	// Read values under the same lock
	entries = make([]KeyValue, 0, len(pageKeys))
	for _, key := range pageKeys {
		entry := m.data[key]
		valueCopy := make([]byte, len(entry.value))
		copy(valueCopy, entry.value)
		entries = append(entries, KeyValue{Key: key, Value: valueCopy})
	}

	return entries, nextToken, hasMore, nil
}

// Close clears the cache. This is a no-op for cleanup purposes.
func (m *MemoryCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]cacheEntry)
	m.versions = make(map[string]uint64)
	m.fences = make(map[string]uint64)
	return nil
}

// GetMode returns ModeSimple since MemoryCache is a single-node implementation.
func (m *MemoryCache) GetMode() ConnectionMode {
	return ModeSimple
}

// GetConnectedNodes returns a single "memory" node identifier.
func (m *MemoryCache) GetConnectedNodes() []string {
	return []string{"memory"}
}

// PutStreamIfVersion buffers the stream and delegates to PutIfVersion (this test
// double keeps everything in memory).
func (m *MemoryCache) PutStreamIfVersion(ctx context.Context, key string, r io.Reader, ttlSeconds int64, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return m.PutIfVersion(ctx, key, data, ttlSeconds, expected)
}

// GetStreamWithVersion writes the value to w and returns its version/presence.
func (m *MemoryCache) GetStreamWithVersion(ctx context.Context, key string, w io.Writer) (uint64, bool, error) {
	data, version, found, err := m.GetWithVersion(ctx, key)
	if err != nil || !found {
		return version, found, err
	}
	if _, err := w.Write(data); err != nil {
		return 0, false, err
	}
	return version, true, nil
}
