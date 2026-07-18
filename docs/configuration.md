# Configuration

OCache can be configured through command-line flags when starting the server.

## Basic Usage

```bash
./ocache [flags]
```

## Configuration Flags

### Network Configuration

| Flag           | Type   | Default | Description                    |
| -------------- | ------ | ------- | ------------------------------ |
| `-listen-addr` | string | `:9000` | gRPC server listen address     |
| `-listen-http` | string | `:9001` | HTTP API server listen address |

### Storage Configuration

| Flag                 | Type   | Default      | Description                                                                                      |
| -------------------- | ------ | ------------ | ------------------------------------------------------------------------------------------------ |
| `-disk`              | string | `/var/cache` | Directory for disk cache storage                                                                 |
| `-threshold`         | int    | 65536        | Small object threshold in bytes (64KB). Objects smaller than this are stored in RocksDB          |
| `-segment-size`      | int64  | 268435456    | Segment size in bytes (256MB) for large object storage                                           |
| `-compact-threshold` | int64  | 16777216     | Compaction threshold in bytes (16MB). Objects less than this are eligible for segment compaction |
| `-max-disk-usage`    | int64  | 0            | Maximum disk usage in bytes (0 = unlimited). When set, enables eviction                          |
| `-eviction-policy`   | string | `lru`        | Eviction order when `-max-disk-usage` is set: `lru` (reads refresh recency) or `fifo` (evict oldest-written first; reads do not protect data) |

> **Eviction policies.** With `-max-disk-usage` set, the cache evicts to stay
> under the cap. `lru` evicts the least-recently-*accessed* key first — a read
> refreshes an entry's position, protecting recently-read data. `fifo` evicts the
> oldest-*written* key first and reads never change an entry's position, so a rare
> read of old data cannot displace hotter data. `fifo` suits write-once workloads
> (e.g. parquet, where the newest data is read most).
>
> Each policy maintains its own eviction index with a per-key back-reference, so
> writes, overwrites, deletes, and TTL expiry keep exactly one entry per live key
> (an overwrite re-indexes the key at its new write time). The index is built as
> keys are written, so **choose the policy and cap at deployment time and keep
> them fixed for the life of the data directory.**
>
> - `fifo` only evicts keys written after it was enabled. Enabling it (or the cap)
>   on a directory that already holds keys leaves those keys unindexed and not
>   evictable, so the cap cannot reclaim them and eviction thrashes newer keys
>   trying to reach a target it can never hit. ocache logs a warning at startup
>   when it detects pre-existing keys with an empty FIFO index.
> - Switching `-eviction-policy` in place (e.g. `lru`↔`fifo`) is not supported:
>   each policy only maintains and prunes its own index, so the previous policy's
>   index rows are never reclaimed. Recreate the data directory instead.

### Cache Configuration

| Flag                   | Type  | Default    | Description                                           |
| ---------------------- | ----- | ---------- | ----------------------------------------------------- |
| `-ttl`                 | int   | 0          | Default TTL in seconds when no key-level TTL is set   |
| `-fd-cache-size`       | int   | 10000      | Size of the file descriptor cache (number of entries) |
| `-metadata-cache-size` | int64 | 1073741824 | Maximum size of the metadata cache in bytes (1GB)     |
| `-metadata-background-jobs` | int | 8 | Max concurrent RocksDB background jobs (compactions + flushes) over the process lifetime |

### Compaction Configuration

| Flag                            | Type     | Default | Description                                                |
| ------------------------------- | -------- | ------- | ---------------------------------------------------------- |
| `-compaction-threads`           | int      | 2       | Number of concurrent compaction threads                    |
| `-fragmentation-threshold`      | float64  | 0.3     | Segment fragmentation threshold for recompaction (0.0-1.0) |
| `-recompaction-min-segment-age` | duration | 1h      | Minimum age for a segment before recompaction              |
| `-recompaction-min-segments`    | int      | 3       | Minimum number of segments required before recompaction    |
| `-disable-recompaction`         | bool     | false   | Disable automatic segment recompaction                     |

### TTL and Cleanup

| Flag                    | Type     | Default | Description                             |
| ----------------------- | -------- | ------- | --------------------------------------- |
| `-ttl-cleanup-interval` | duration | 5m      | Interval between TTL cleanup operations |

### Cluster Configuration

| Flag                  | Type     | Default | Description                                                                    |
| --------------------- | -------- | ------- | ------------------------------------------------------------------------------ |
| `-cluster-enabled`    | bool     | false   | Enable cluster mode for distributed caching                                    |
| `-node-id`            | string   | ""      | Unique node identifier (required in cluster mode)                              |
| `-cluster-addr`       | string   | `:7000` | Address for internal cluster communication                                     |
| `-seeds`              | string   | ""      | Comma-separated seed nodes for cluster discovery (e.g., node1:7000,node2:7000) |
| `-partition-count`    | int      | 16384   | Number of partitions in the consistent hash ring                               |
| `-heartbeat-interval` | duration | 5s      | Interval between heartbeat messages                                            |
| `-failure-threshold`  | int      | 3       | Number of missed heartbeats before marking a node as down                      |

### Logging

| Flag               | Type | Default | Description                  |
| ------------------ | ---- | ------- | ---------------------------- |
| `-v`               | bool | false   | Enable verbose/debug logging |
| `-request-logging` | bool | false   | Enable request logging       |

## Configuration Examples

### Development Setup

Low TTL, verbose logging, custom directories:

```bash
./ocache \
  -disk /tmp/ocache-dev \
  -ttl 60 \
  -v
```

### Single Node Production Setup

Specific disk limits (1TB):

```bash
./ocache \
  -disk /var/cache/ocache \
  -listen-addr :9000 \
  -listen-http :9001 \
  -max-disk-usage 1000000000000
```

### Cluster Mode Setup

Three-node cluster configuration assuming the nodes are part of the service domain `ocache.svc.cluster.local`:

**Node 1:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node1 \
  -cluster-addr :7000 \
  -seeds "ocache.svc.cluster.local:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache
```

**Node 2:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node2 \
  -cluster-addr :7000 \
  -seeds "ocache.svc.cluster.local:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache
```

**Node 3:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node3 \
  -cluster-addr :7000 \
  -seeds "ocache.svc.cluster.local:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache
```

## Storage Strategy

OCache uses a dual-storage strategy:

1. **Small Objects** (< threshold):

   - Stored directly in RocksDB
   - Fast access, lower overhead
   - Good for metadata, small files, JSON objects

2. **Large Objects** (>= threshold):
   - Stored on disk in segmented files
   - Metadata in RocksDB
   - Efficient for large files, images, videos

## System Requirements

### Hardware Recommendations

**Per Node:**

- CPU: 2-4 cores
- RAM: 4-8GB
- Disk: NVMe SSD preferred for high performance
- Network: 10-25 Gbps

## Performance Tuning

### Segment Size Tuning

- **Smaller segments** (64MB-128MB): More files, better for mixed workloads
- **Default segments** (256MB): Balanced file management
- **Larger segments** (512MB-1GB): Fewer files, better for large object workloads

### FD Cache Size

- Increase for workloads with many concurrent large object reads
- Default (10000) is suitable for most workloads
- Monitor file descriptor usage and adjust accordingly

### Compaction Tuning

**Compaction Threshold:**

- Controls which objects are eligible for segment compaction
- Lower values (8MB): Less compaction overhead, more raw files
- Default (16MB): Balanced approach
- Higher values (32MB+): More objects compacted, better space efficiency

**Compaction Threads:**

- Number of concurrent compaction workers
- Default (1): Good for most systems
- High (2-4): Faster compaction on multi-core systems

### Recompaction Tuning

**Fragmentation Threshold:**

- Triggers recompaction when segment waste exceeds this ratio
- Lower values (0.1-0.2): Aggressive space reclamation
- Default (0.5): 50% waste tolerance
- Higher values (0.7-0.8): Less recompaction overhead

**Minimum Segment Age:**

- Prevents recompacting recently created segments
- Default (2h): Allows segments to stabilize
- Shorter (1h): More aggressive recompaction
- Longer (6h+): Very stable segments only

### Cluster Tuning

**Partition Count:**

- Number of hash ring partitions
- Lower (1024-4096): Coarser distribution
- Default (16384): Good balance for most clusters
- Higher (32768+): Better distribution for large clusters

**Heartbeat Interval:**

- Frequency of health checks between nodes
- Shorter (1-3s): Faster failure detection, more network pings
- Default (5s): Balanced detection time
- Longer (10s+): Lower overhead, slower detection

**Failure Threshold:**

- Missed heartbeats before marking node down
- Lower (1-2): Very fast failure detection, risk of false positives
- Default (3): Good balance
- Higher (5+): More tolerance for network issues

### Large-object workloads (e.g. SlateDB SSTs)

For workloads dominated by large objects (hundreds of MB, such as SlateDB SSTs cached whole), tune so large objects stay on the streamed raw-file path and never enter the compaction/segment machinery:

- **Keep large objects as raw files.** Objects larger than `-compact-threshold` (default 64 MB) are stored as permanent raw files and never compacted — correct for large SSTs. Keep `-compact-threshold` below your object sizes so they take this path. It must also stay **below** `-segment-size` (default 256 MB); ocache clamps and warns if `compact-threshold >= segment-size`, since an object larger than a segment cannot be compacted into one.
- **Size the block cache and background CPU to the container limit.** `-metadata-cache-size` bounds the RocksDB block cache. `-recovery-workers` bounds the one-time startup file-recovery parallelism. `-metadata-background-jobs` caps RocksDB's background compaction/flush threads for the **whole process lifetime** (boot *and* steady state) — useful to bound background CPU on a container that does not set a CPU limit (a blanket CPU limit would also throttle request handling). Note that `GOMAXPROCS` governs only the Go scheduler, not RocksDB's own thread pools, so lowering it does not reduce this background CPU — use `-metadata-background-jobs`.
- **Get/Put/range are streamed** and handle large objects without buffering the whole object. **List-with-values** is the exception: it caps returned values at 1 MiB per value — larger values are omitted from the response (key and size are still returned, `value_omitted=true`) so a List over large objects cannot buffer object-sized allocations or exceed the gRPC message limit. Use keys-only `List` when you only need keys.
- **Readiness/boot:** a warm-cache boot over a large on-disk store can be slow; add a Kubernetes `startupProbe` so it is not liveness-killed mid-boot (see [Node lifecycle & readiness gating](cluster.md#readiness-gating)).

## Monitoring

Enable verbose logging (`-v`) to monitor:

- Cache hits/misses
- Storage operations
- Compaction events
- TTL expiration
- File descriptor usage

## Environment Variables

Currently, OCache uses command-line flags exclusively. Environment variable support may be added in future versions.

## Configuration File Support

Configuration file support is planned for future releases to allow more complex configurations and easier deployment management.
