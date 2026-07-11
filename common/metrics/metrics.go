// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Histogram buckets milliseconds
	fastOpsBuckets = []float64{
		0.5,   // 500μs
		1,     // 1ms
		2.5,   // 2.5ms
		5,     // 5ms
		10,    // 10ms
		25,    // 25ms
		50,    // 50ms
		100,   // 100ms
		250,   // 250ms
		500,   // 500ms
		1000,  // 1s
		2500,  // 2.5s
		5000,  // 5s
		10000, // 10s
	}
	longOpsBuckets = []float64{
		100,    // 100ms
		250,    // 250ms
		500,    // 500ms
		1000,   // 1s
		2500,   // 2.5s
		5000,   // 5s
		10000,  // 10s
		30000,  // 30s
		60000,  // 1m
		120000, // 2m
		300000, // 5m
		600000, // 10m
	}

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
			Buckets: fastOpsBuckets,
		},
		[]string{"method"},
	)

	// GRPCPanicsRecovered counts panics recovered by the gRPC recovery
	// interceptors. A non-zero value means a handler panicked and the RPC was
	// failed in isolation instead of crashing the process.
	GRPCPanicsRecovered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_grpc_panics_recovered_total",
			Help: "Total number of panics recovered by the gRPC recovery interceptors",
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
			Buckets: fastOpsBuckets,
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
			Buckets: longOpsBuckets,
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
			Buckets: longOpsBuckets,
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

	FDCacheNotCached = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_fd_cache_not_cached_total",
			Help: "Total number of file descriptor cache not cached",
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

	// ListValuesOmitted counts values omitted from List-with-values responses
	// because they exceeded the per-value size cap (keys/sizes are still returned).
	ListValuesOmitted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_list_values_omitted_total",
			Help: "Number of values omitted from List-with-values responses for exceeding the per-value size cap",
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
			Buckets: longOpsBuckets,
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

	// Deletion Queue Metrics
	DeletionQueueAdded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_deletion_queue_added_total",
			Help: "Total number of files added to deletion queue",
		},
	)

	DeletionQueueProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_deletion_queue_processed_total",
			Help: "Total number of files successfully deleted from queue",
		},
	)

	DeletionQueueFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_deletion_queue_failed_total",
			Help: "Total number of failed deletion attempts",
		},
	)

	DeletionQueuePruned = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_deletion_queue_pruned_total",
			Help: "Total number of old entries pruned from queue",
		},
	)

	DeletionQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_deletion_queue_depth",
			Help: "Current number of files pending deletion",
		},
	)

	DeletionQueueBatchDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ocache_deletion_queue_batch_duration_ms",
			Help:    "Deletion queue batch processing duration in milliseconds",
			Buckets: longOpsBuckets,
		},
	)

	// Recompaction Metrics
	RecompactionRuns = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recompaction_runs_total",
			Help: "Total number of recompaction runs",
		},
	)

	RecompactionSegments = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recompaction_segments_total",
			Help: "Total number of segments recompacted",
		},
	)

	RecompactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ocache_recompaction_duration_ms",
			Help:    "Recompaction duration in milliseconds",
			Buckets: longOpsBuckets,
		},
	)

	RecompactionEntriesCopied = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recompaction_entries_copied_total",
			Help: "Total number of entries copied during recompaction",
		},
	)

	RecompactionBytesCopied = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recompaction_bytes_copied_total",
			Help: "Total bytes copied during recompaction",
		},
	)

	RecompactionBytesFreed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_recompaction_bytes_freed_total",
			Help: "Total bytes freed by recompaction",
		},
	)

	// Cluster Membership Metrics
	ClusterNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_nodes",
			Help: "Number of nodes in the cluster by status",
		},
		[]string{"status"},
	)

	ClusterEpoch = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_epoch",
			Help: "Current cluster membership epoch",
		},
	)

	ClusterPartitionCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_partition_count",
			Help: "Total number of partitions in the hash ring",
		},
	)

	ClusterNodesAdded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cluster_nodes_added_total",
			Help: "Total number of nodes added to the cluster",
		},
	)

	ClusterNodesRemoved = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cluster_nodes_removed_total",
			Help: "Total number of nodes removed from the cluster",
		},
	)

	ClusterTokensOwned = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_tokens_owned",
			Help: "Number of tokens owned by this instance",
		},
	)

	// Heartbeat & Failure Detection Metrics
	ClusterHeartbeatsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_heartbeats_sent_total",
			Help: "Total number of heartbeats sent to each node",
		},
		[]string{"target_node"},
	)

	ClusterHeartbeatsReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_heartbeats_received_total",
			Help: "Total number of heartbeats received from each node",
		},
		[]string{"source_node"},
	)

	ClusterHeartbeatFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_heartbeat_failures_total",
			Help: "Total number of failed heartbeat attempts per node",
		},
		[]string{"target_node"},
	)

	ClusterHeartbeatDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ocache_cluster_heartbeat_duration_ms",
			Help:    "Heartbeat round-trip time in milliseconds",
			Buckets: fastOpsBuckets,
		},
		[]string{"target_node"},
	)

	ClusterNodeFailureCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_node_failure_count",
			Help: "Current consecutive failure count for each node",
		},
		[]string{"node_id"},
	)

	ClusterNodesMarkedDown = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cluster_nodes_marked_down_total",
			Help: "Total number of times nodes were marked as down",
		},
	)

	// Router & Connection Metrics
	ClusterRouteRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_route_requests_total",
			Help: "Total number of routing requests by result type",
		},
		[]string{"result"},
	)

	ClusterConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_connections_active",
			Help: "Number of active gRPC connections to each node",
		},
		[]string{"node_id"},
	)

	ClusterConnectionFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_connection_failures_total",
			Help: "Total number of connection failures per node",
		},
		[]string{"node_id", "reason"},
	)

	ClusterCircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocache_cluster_circuit_breaker_state",
			Help: "Circuit breaker state per node (0=closed, 1=open)",
		},
		[]string{"node_id"},
	)

	ClusterCircuitBreakerOpened = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_circuit_breaker_opened_total",
			Help: "Total number of times circuit breaker opened per node",
		},
		[]string{"node_id"},
	)

	ClusterRetryAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_retry_attempts_total",
			Help: "Total number of retry attempts for failed routes",
		},
		[]string{"node_id"},
	)

	ClusterRoutingErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_routing_errors_total",
			Help: "Total number of routing errors by type",
		},
		[]string{"error_type"},
	)

	// Join/Sync Operations Metrics
	ClusterJoinRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_join_requests_total",
			Help: "Total number of join requests handled",
		},
		[]string{"status"},
	)

	ClusterSyncOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_sync_operations_total",
			Help: "Total number of cluster state synchronizations",
		},
		[]string{"status"},
	)

	ClusterSyncDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ocache_cluster_sync_duration_ms",
			Help:    "Time to synchronize cluster state with a node in milliseconds",
			Buckets: fastOpsBuckets,
		},
		[]string{"target_node"},
	)

	ClusterBroadcastsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_broadcasts_sent_total",
			Help: "Total number of broadcast operations sent",
		},
		[]string{"type"},
	)

	ClusterBroadcastsDuplicate = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cluster_broadcasts_duplicate_total",
			Help: "Total number of duplicate broadcasts prevented",
		},
	)

	// Discovery Metrics
	ClusterDiscoveryRefreshes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_discovery_refreshes_total",
			Help: "Total number of node discovery refresh attempts",
		},
		[]string{"status"},
	)

	ClusterDiscoveryNodesChanged = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_discovery_nodes_changed_total",
			Help: "Total number of node changes detected during discovery",
		},
		[]string{"type"},
	)

	ClusterDiscoveryDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ocache_cluster_discovery_duration_ms",
			Help:    "Time to resolve nodes via discovery in milliseconds",
			Buckets: fastOpsBuckets,
		},
	)

	// Ring/Partition Metrics
	ClusterKeyLookups = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ocache_cluster_key_lookups_total",
			Help: "Total number of key-to-node lookups performed",
		},
	)

	ClusterLocalKeyChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ocache_cluster_local_key_checks_total",
			Help: "Total number of IsLocal checks performed",
		},
		[]string{"result"},
	)
)

// Init initializes the metrics package
func Init() {
	// Register all metrics with Prometheus
	// This is automatically done by promauto, but we can add
	// any additional initialization here if needed
}
