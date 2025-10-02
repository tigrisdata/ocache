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
| `-max-disk-usage`    | int64  | 0            | Maximum disk usage in bytes (0 = unlimited). When set, uses LRU eviction                         |

### Cache Configuration

| Flag                   | Type  | Default    | Description                                           |
| ---------------------- | ----- | ---------- | ----------------------------------------------------- |
| `-ttl`                 | int   | 0          | Default TTL in seconds when no key-level TTL is set   |
| `-fd-cache-size`       | int   | 10000      | Size of the file descriptor cache (number of entries) |
| `-metadata-cache-size` | int64 | 1073741824 | Maximum size of the metadata cache in bytes (1GB)     |

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
