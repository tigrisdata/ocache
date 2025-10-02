package integration

import (
	"time"
)

// TestHarnessInterface defines the common interface for both single-node and cluster test harnesses
type TestHarnessInterface interface {
	// Object operations
	PutObject(key string, data []byte, ttl int64) error
	PutObjectStream(key string, data []byte, ttl int64) error // For large objects >128MB
	GetObject(key string) ([]byte, error)
	DeleteObject(key string) error

	// Storage inspection
	GetStorageStats() StorageStats

	// Lifecycle
	Cleanup()
	PrintMetrics()

	// Test context
	GetTempDir() string
}

// TestWaitForCleanup waits for cleanup to run (optional interface)
type TestWaitForCleanup interface {
	WaitForCleanup(timeout time.Duration) error
}

// TestWaitForCompaction waits for compaction to run (optional interface)
type TestWaitForCompaction interface {
	WaitForCompaction(timeout time.Duration) error
}

// TestStorageAccess provides direct storage access (optional interface for single-node tests)
type TestStorageAccess interface {
	GetStorage() interface{} // Returns *storage.Storage for single-node harness
	SetAccessTime(key string, timestamp int64)
	FlushAccessUpdates()
}
