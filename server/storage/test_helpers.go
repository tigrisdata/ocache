package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/server/storage/metadata"
)

// createTestStorage creates a new storage instance for testing and returns a cleanup function
func createTestStorage(t testing.TB, ttl int, inlineThreshold int, compactThreshold int64,
	segmentSize int64, fdCacheSize int, maxDiskUsage int64,
) (*Storage, func()) {
	dir := t.TempDir()
	s, err := newStorage(dir, ttl, inlineThreshold, compactThreshold, segmentSize, fdCacheSize, maxDiskUsage)
	require.NoError(t, err, "failed to create storage")

	cleanup := func() {
		// Stop background services
		if s.syncMonitor != nil {
			s.syncMonitor.Stop()
		}
		if s.accessUpdater != nil {
			s.accessUpdater.Stop()
		}
		if s.cleaner != nil {
			s.cleaner.Close()
		}
		if s.compactor != nil {
			s.compactor.Close()
		}
		if s.segmentManager != nil {
			s.segmentManager.Close()
		}
		// Close the metadata DB to reset the global instance
		// This is critical to prevent test data leaking between tests
		metadata.CloseMetaDB()
	}

	return s, cleanup
}

// createTestStorageWithDefaults creates a test storage with common default values
func createTestStorageWithDefaults(t testing.TB) (*Storage, func()) {
	return createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
}
