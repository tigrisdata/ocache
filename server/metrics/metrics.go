package metrics

// Re-export all metrics from the common package to maintain backward compatibility
// This allows the server package to continue using server/metrics without changes

import (
	"github.com/tigrisdata/ocache/common/metrics"
)

var (
	// API Metrics
	RPCRequests = metrics.RPCRequests
	RPCDuration = metrics.RPCDuration

	// Storage Metrics
	StorageOperations        = metrics.StorageOperations
	StorageOperationDuration = metrics.StorageOperationDuration
	StorageBytes             = metrics.StorageBytes
	ObjectSize               = metrics.ObjectSize

	// Segment Metrics
	SegmentCount         = metrics.SegmentCount
	SegmentSize          = metrics.SegmentSize
	SegmentFragmentation = metrics.SegmentFragmentation

	// Compaction Metrics
	CompactionRuns           = metrics.CompactionRuns
	CompactionDuration       = metrics.CompactionDuration
	CompactionBytesCompacted = metrics.CompactionBytesCompacted
	CompactionFilesCompacted = metrics.CompactionFilesCompacted

	// Cleaner Metrics
	CleanerRuns        = metrics.CleanerRuns
	CleanerDuration    = metrics.CleanerDuration
	CleanerKeysDeleted = metrics.CleanerKeysDeleted
	CleanerBytesFreed  = metrics.CleanerBytesFreed

	// Disk Usage Metrics
	DiskUsageBytes = metrics.DiskUsageBytes
	DiskUsageRatio = metrics.DiskUsageRatio

	// LRU Metrics
	LRUEvictions     = metrics.LRUEvictions
	LRUAccessUpdates = metrics.LRUAccessUpdates

	// File Descriptor Metrics
	FDCacheHits      = metrics.FDCacheHits
	FDCacheMisses    = metrics.FDCacheMisses
	FDCacheEvictions = metrics.FDCacheEvictions
	FDCacheSize      = metrics.FDCacheSize

	// Stream Metrics
	StreamsActive          = metrics.StreamsActive
	StreamBytesTransferred = metrics.StreamBytesTransferred

	// Error Metrics
	Errors = metrics.Errors

	// System Metrics
	KeysTotal         = metrics.KeysTotal
	BytesTotal        = metrics.BytesTotal
	ConnectionsActive = metrics.ConnectionsActive

	// Buffer Pool Metrics
	BufferPoolAllocations = metrics.BufferPoolAllocations
	BufferPoolReleases    = metrics.BufferPoolReleases
	BufferPoolSize        = metrics.BufferPoolSize

	// Recovery Metrics
	RecoveryRuns          = metrics.RecoveryRuns
	RecoveryDuration      = metrics.RecoveryDuration
	RecoveryKeysRecovered = metrics.RecoveryKeysRecovered

	// Cache Hit/Miss metrics
	CacheHits   = metrics.CacheHits
	CacheMisses = metrics.CacheMisses

	// RocksDB specific metrics
	RocksDBOperations        = metrics.RocksDBOperations
	RocksDBOperationDuration = metrics.RocksDBOperationDuration
)

// Init initializes the metrics package
func Init() {
	metrics.Init()
}
