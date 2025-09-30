package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// createTestStorage creates a new storage instance for testing and returns a cleanup function
func createTestStorage(t testing.TB, ttl int, inlineThreshold int, compactThreshold int64,
	segmentSize int64, fdCacheSize int, maxDiskUsage int64,
) (*Storage, func()) {
	dir := t.TempDir()

	config := &StorageConfig{
		DiskPath:            dir,
		TTL:                 ttl,
		InlineThreshold:     inlineThreshold,
		CompactThreshold:    compactThreshold,
		SegmentSize:         segmentSize,
		FdCacheSize:         fdCacheSize,
		MaxDiskUsage:        maxDiskUsage,
		CompactionInterval:  DefaultCompactionInterval,
		FragThreshold:       DefaultFragmentationThreshold,
		DisableRecompaction: true,
	}
	s, err := NewStorageWithConfig(config)
	require.NoError(t, err, "failed to create storage")

	cleanup := func() {
		// Use the storage's Close method to ensure proper shutdown order
		// and complete synchronization of all background operations
		s.Close()
	}

	return s, cleanup
}

// createTestStorageWithDefaults creates a test storage with common default values
func createTestStorageWithDefaults(t testing.TB) (*Storage, func()) {
	return createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
}
