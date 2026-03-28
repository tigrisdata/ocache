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
}

// cacheEntry holds a cached value with optional expiration.
type cacheEntry struct {
	value     []byte
	expiresAt time.Time // Zero value means no expiration
}

// Compile-time check that MemoryCache implements CacheClient.
var _ CacheClient = (*MemoryCache)(nil)

// NewMemoryCache creates a new in-memory cache.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		data: make(map[string]cacheEntry),
	}
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
		delete(m.data, key)
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
		delete(m.data, key)
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

	delete(m.data, key)
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
