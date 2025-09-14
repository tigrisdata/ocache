Prometheus Metrics

This document provides a comprehensive reference for all Prometheus metrics exposed by OCache.

## Metrics Endpoint

OCache exposes Prometheus metrics at the `/metrics` endpoint on the configured HTTP port (default: 9001).

## Metric Categories

### API Metrics

| Metric Name                 | Type      | Labels             | Description                                                                                                                                                                    |
| --------------------------- | --------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ocache_rpc_requests_total` | Counter   | `method`, `status` | Total number of RPC requests received. The `method` label indicates the RPC method name (e.g., PutObject, GetObject, DeleteObject), and `status` indicates success or failure. |
| `ocache_rpc_duration_ms`    | Histogram | `method`           | RPC request duration in milliseconds. Tracks the latency of each RPC method with buckets ranging from 0.5ms to 10s.                                                            |

### Storage Operation Metrics

| Metric Name                            | Type      | Labels                                | Description                                                                                                                                                |
| -------------------------------------- | --------- | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ocache_storage_operations_total`      | Counter   | `operation`, `storage_type`, `status` | Total number of storage operations. `operation` can be get/put/delete, `storage_type` can be rocksdb/file/segment, and `status` indicates success/failure. |
| `ocache_storage_operation_duration_ms` | Histogram | `operation`, `storage_type`           | Storage operation duration in milliseconds. Measures the latency of storage operations by type.                                                            |
| `ocache_storage_bytes_total`           | Counter   | `operation`, `storage_type`           | Total bytes stored or retrieved. Tracks data volume by operation (put/get) and storage type.                                                               |
| `ocache_object_size_bytes`             | Histogram | `operation`                           | Distribution of object sizes in bytes. Buckets range from 1KB to 1GB to understand object size patterns.                                                   |

### Segment Storage Metrics

| Metric Name                          | Type  | Labels | Description                                                                                                       |
| ------------------------------------ | ----- | ------ | ----------------------------------------------------------------------------------------------------------------- |
| `ocache_segments_total`              | Gauge | -      | Total number of segments currently in the system.                                                                 |
| `ocache_segment_size_bytes`          | Gauge | -      | Total size of all segments in bytes.                                                                              |
| `ocache_segment_fragmentation_ratio` | Gauge | -      | Segment fragmentation ratio (0-1). Higher values indicate more fragmentation and potential need for recompaction. |

### Compaction Metrics

| Metric Name                               | Type      | Labels | Description                                                                       |
| ----------------------------------------- | --------- | ------ | --------------------------------------------------------------------------------- |
| `ocache_compaction_runs_total`            | Counter   | -      | Total number of compaction runs executed.                                         |
| `ocache_compaction_duration_ms`           | Histogram | -      | Compaction duration in milliseconds. Uses longer duration buckets (100ms to 10m). |
| `ocache_compaction_bytes_compacted_total` | Counter   | -      | Total bytes compacted from raw files to segments.                                 |
| `ocache_compaction_files_compacted_total` | Counter   | -      | Total number of files compacted into segments.                                    |

### Recompaction Metrics

| Metric Name                                | Type      | Labels | Description                                                |
| ------------------------------------------ | --------- | ------ | ---------------------------------------------------------- |
| `ocache_recompaction_runs_total`           | Counter   | -      | Total number of recompaction runs to defragment segments.  |
| `ocache_recompaction_segments_total`       | Counter   | -      | Total number of segments recompacted.                      |
| `ocache_recompaction_duration_ms`          | Histogram | -      | Recompaction duration in milliseconds.                     |
| `ocache_recompaction_entries_copied_total` | Counter   | -      | Total number of entries copied during recompaction.        |
| `ocache_recompaction_bytes_copied_total`   | Counter   | -      | Total bytes copied during recompaction.                    |
| `ocache_recompaction_bytes_freed_total`    | Counter   | -      | Total bytes freed by recompaction through defragmentation. |

### Cleaner Metrics

| Metric Name                         | Type      | Labels           | Description                                                           |
| ----------------------------------- | --------- | ---------------- | --------------------------------------------------------------------- |
| `ocache_cleaner_runs_total`         | Counter   | `type`           | Total number of cleaner runs. Type can be `ttl` or `lru`.             |
| `ocache_cleaner_duration_ms`        | Histogram | `type`           | Cleaner run duration in milliseconds by type.                         |
| `ocache_cleaner_keys_deleted_total` | Counter   | `type`, `reason` | Total number of keys deleted. `reason` can be `expired` or `evicted`. |
| `ocache_cleaner_bytes_freed_total`  | Counter   | `type`           | Total bytes freed by cleaner type.                                    |

### Disk Usage Metrics

| Metric Name               | Type  | Labels | Description                                                               |
| ------------------------- | ----- | ------ | ------------------------------------------------------------------------- |
| `ocache_disk_usage_bytes` | Gauge | `type` | Current disk usage in bytes. Type can be `files`, `segments`, or `total`. |
| `ocache_disk_usage_ratio` | Gauge | -      | Disk usage ratio (0-1) relative to configured maximum.                    |

### LRU Cache Metrics

| Metric Name                       | Type    | Labels | Description                                                      |
| --------------------------------- | ------- | ------ | ---------------------------------------------------------------- |
| `ocache_lru_evictions_total`      | Counter | -      | Total number of LRU evictions when disk usage exceeds threshold. |
| `ocache_lru_access_updates_total` | Counter | -      | Total number of LRU access time updates.                         |

### File Descriptor Cache Metrics

| Metric Name                        | Type    | Labels | Description                                                       |
| ---------------------------------- | ------- | ------ | ----------------------------------------------------------------- |
| `ocache_fd_cache_hits_total`       | Counter | -      | Total number of file descriptor cache hits.                       |
| `ocache_fd_cache_misses_total`     | Counter | -      | Total number of file descriptor cache misses requiring file open. |
| `ocache_fd_cache_evictions_total`  | Counter | -      | Total number of file descriptor cache evictions.                  |
| `ocache_fd_cache_not_cached_total` | Counter | -      | Total number of files not cached (e.g., too large).               |
| `ocache_fd_cache_size`             | Gauge   | -      | Current number of cached file descriptors.                        |

### Streaming Metrics

| Metric Name                             | Type    | Labels      | Description                                                                     |
| --------------------------------------- | ------- | ----------- | ------------------------------------------------------------------------------- |
| `ocache_streams_active`                 | Gauge   | -           | Number of active streaming operations.                                          |
| `ocache_stream_bytes_transferred_total` | Counter | `direction` | Total bytes transferred via streaming. Direction can be `upload` or `download`. |

### Cache Performance Metrics

| Metric Name                 | Type    | Labels | Description                                   |
| --------------------------- | ------- | ------ | --------------------------------------------- |
| `ocache_cache_hits_total`   | Counter | -      | Total number of cache hits (successful gets). |
| `ocache_cache_misses_total` | Counter | -      | Total number of cache misses (key not found). |

### Deletion Queue Metrics

| Metric Name                               | Type      | Labels | Description                                               |
| ----------------------------------------- | --------- | ------ | --------------------------------------------------------- |
| `ocache_deletion_queue_added_total`       | Counter   | -      | Total number of files added to deletion queue.            |
| `ocache_deletion_queue_processed_total`   | Counter   | -      | Total number of files successfully deleted from queue.    |
| `ocache_deletion_queue_failed_total`      | Counter   | -      | Total number of failed deletion attempts.                 |
| `ocache_deletion_queue_pruned_total`      | Counter   | -      | Total number of old entries pruned from queue.            |
| `ocache_deletion_queue_depth`             | Gauge     | -      | Current number of files pending deletion.                 |
| `ocache_deletion_queue_batch_duration_ms` | Histogram | -      | Deletion queue batch processing duration in milliseconds. |

### Buffer Pool Metrics

| Metric Name                            | Type    | Labels | Description                                   |
| -------------------------------------- | ------- | ------ | --------------------------------------------- |
| `ocache_buffer_pool_allocations_total` | Counter | -      | Total number of buffer pool allocations.      |
| `ocache_buffer_pool_releases_total`    | Counter | -      | Total number of buffer pool releases.         |
| `ocache_buffer_pool_size`              | Gauge   | -      | Current buffer pool size (number of buffers). |

### Recovery Metrics

| Metric Name                            | Type      | Labels | Description                               |
| -------------------------------------- | --------- | ------ | ----------------------------------------- |
| `ocache_recovery_runs_total`           | Counter   | -      | Total number of recovery runs at startup. |
| `ocache_recovery_duration_ms`          | Histogram | -      | Recovery duration in milliseconds.        |
| `ocache_recovery_keys_recovered_total` | Counter   | -      | Total number of keys recovered from disk. |

### Error Metrics

| Metric Name           | Type    | Labels              | Description                                                                                                                                    |
| --------------------- | ------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `ocache_errors_total` | Counter | `type`, `operation` | Total number of errors. `type` indicates error category (e.g., storage, network, corruption), `operation` indicates the operation that failed. |

### System Metrics

| Metric Name                 | Type  | Labels | Description                                                 |
| --------------------------- | ----- | ------ | ----------------------------------------------------------- |
| `ocache_keys_total`         | Gauge | -      | Total number of keys currently in cache.                    |
| `ocache_bytes_total`        | Gauge | -      | Total bytes currently stored in cache.                      |
| `ocache_connections_active` | Gauge | `type` | Number of active connections. Type can be `grpc` or `http`. |

## Histogram Buckets

OCache uses two sets of histogram buckets optimized for different operation types:

### Fast Operations Buckets (used for most operations)

- 0.5ms, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s

### Long Operations Buckets (used for compaction, cleaner, recovery)

- 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 2m, 5m, 10m

### Object Size Buckets

- 1KB, 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB, 256MB, 1GB
