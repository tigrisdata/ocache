package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// API Metrics
	RPCRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_rpc_requests_total",
			Help: "Total number of RPC requests",
		},
		[]string{"method", "status"},
	)

	RPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ocache_rpc_duration_ms",
			Help:    "RPC request duration in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// Storage Metrics
	StorageOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_storage_operations_total",
			Help: "Total number of storage operations",
		},
		[]string{"operation", "storage_type", "status"},
	)

	StorageOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ocache_storage_operation_duration_ms",
			Help:    "Storage operation duration in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "storage_type"},
	)

	StorageBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_storage_bytes_total",
			Help: "Total bytes stored or retrieved",
		},
		[]string{"operation", "storage_type"},
	)

	ObjectSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ocache_object_size_bytes",
			Help: "Distribution of object sizes in bytes",
			Buckets: []float64{
				1024,       // 1KB
				4096,       // 4KB
				16384,      // 16KB
				65536,      // 64KB
				262144,     // 256KB
				1048576,    // 1MB
				4194304,    // 4MB
				16777216,   // 16MB
				67108864,   // 64MB
				268435456,  // 256MB
				1073741824, // 1GB
			},
		},
		[]string{"operation"},
	)

	// Segment Metrics
	SegmentCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_segments_total",
			Help: "Total number of segments",
		},
	)

	SegmentSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_segment_size_bytes",
			Help: "Total size of all segments in bytes",
		},
	)

	SegmentFragmentation = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_segment_fragmentation_ratio",
			Help: "Segment fragmentation ratio (0-1)",
		},
	)

	// Compaction Metrics
	CompactionRuns = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_compaction_runs_total",
			Help: "Total number of compaction runs",
		},
	)

	CompactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ocache_compaction_duration_ms",
			Help:    "Compaction duration in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	CompactionBytesCompacted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_compaction_bytes_compacted_total",
			Help: "Total bytes compacted",
		},
	)

	CompactionFilesCompacted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_compaction_files_compacted_total",
			Help: "Total number of files compacted",
		},
	)

	// Cleaner Metrics
	CleanerRuns = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cleaner_runs_total",
			Help: "Total number of cleaner runs",
		},
		[]string{"type"},
	)

	CleanerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ocache_cleaner_duration_ms",
			Help:    "Cleaner run duration in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	CleanerKeysDeleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cleaner_keys_deleted_total",
			Help: "Total number of keys deleted by cleaner",
		},
		[]string{"type", "reason"},
	)

	CleanerBytesFreed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cleaner_bytes_freed_total",
			Help: "Total bytes freed by cleaner",
		},
		[]string{"type"},
	)

	// Disk Usage Metrics
	DiskUsageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_disk_usage_bytes",
			Help: "Current disk usage in bytes",
		},
		[]string{"type"},
	)

	DiskUsageRatio = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_disk_usage_ratio",
			Help: "Disk usage ratio (0-1)",
		},
	)

	// LRU Metrics
	LRUEvictions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_lru_evictions_total",
			Help: "Total number of LRU evictions",
		},
	)

	LRUAccessUpdates = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_lru_access_updates_total",
			Help: "Total number of LRU access updates",
		},
	)

	// File Descriptor Metrics
	FDCacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_fd_cache_hits_total",
			Help: "Total number of file descriptor cache hits",
		},
	)

	FDCacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_fd_cache_misses_total",
			Help: "Total number of file descriptor cache misses",
		},
	)

	FDCacheEvictions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_fd_cache_evictions_total",
			Help: "Total number of file descriptor cache evictions",
		},
	)

	FDCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_fd_cache_size",
			Help: "Current file descriptor cache size",
		},
	)

	// Stream Metrics
	StreamsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_streams_active",
			Help: "Number of active streaming operations",
		},
	)

	StreamBytesTransferred = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_stream_bytes_transferred_total",
			Help: "Total bytes transferred via streaming",
		},
		[]string{"direction"},
	)

	// Error Metrics
	Errors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_errors_total",
			Help: "Total number of errors",
		},
		[]string{"type", "operation"},
	)

	// System Metrics
	KeysTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_keys_total",
			Help: "Total number of keys in cache",
		},
	)

	BytesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_bytes_total",
			Help: "Total bytes stored in cache",
		},
	)

	ConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_connections_active",
			Help: "Number of active connections",
		},
		[]string{"type"},
	)

	// Buffer Pool Metrics
	BufferPoolAllocations = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_buffer_pool_allocations_total",
			Help: "Total number of buffer pool allocations",
		},
	)

	BufferPoolReleases = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_buffer_pool_releases_total",
			Help: "Total number of buffer pool releases",
		},
	)

	BufferPoolSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_buffer_pool_size",
			Help: "Current buffer pool size",
		},
	)

	// Recovery Metrics
	RecoveryRuns = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recovery_runs_total",
			Help: "Total number of recovery runs",
		},
	)

	RecoveryDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ocache_recovery_duration_ms",
			Help:    "Recovery duration in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	RecoveryKeysRecovered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recovery_keys_recovered_total",
			Help: "Total number of keys recovered",
		},
	)

	// Cache Hit/Miss metrics
	CacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	CacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)
)

// Init initializes the metrics package
func Init() {
	// Register all metrics with Prometheus
	// This is automatically done by promauto, but we can add
	// any additional initialization here if needed
}
