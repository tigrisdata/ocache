# Configuration

OCache can be configured through command-line flags when starting the server.

## Basic Usage

```bash
./ocache [flags]
```

## Configuration Flags

### Network Configuration

| Flag         | Type | Default | Description          |
| ------------ | ---- | ------- | -------------------- |
| `-port`      | int  | 9000    | gRPC server port     |
| `-http-port` | int  | 9001    | HTTP API server port |

### Storage Configuration

| Flag                 | Type   | Default      | Description                                                                             |
| -------------------- | ------ | ------------ | --------------------------------------------------------------------------------------- |
| `-disk`              | string | `/var/cache` | Directory for disk cache storage                                                        |
| `-threshold`         | int    | 65536        | Small object threshold in bytes (64KB). Objects smaller than this are stored in RocksDB |
| `-segment-size`      | int    | 268435456    | Segment size in bytes (256MB) for large object storage                                  |
| `-compact-threshold` | int    | 16777216     | Compaction threshold in bytes (16MB)                                                    |
| `-max-disk-usage`    | int    | 0            | Maximum disk usage in bytes (0 = unlimited). When set, uses LRU eviction                |

### Cache Configuration

| Flag                   | Type | Default    | Description                                           |
| ---------------------- | ---- | ---------- | ----------------------------------------------------- |
| `-ttl`                 | int  | 0          | Default TTL in seconds when no key-level TTL is set   |
| `-fd-cache-size`       | int  | 10000      | Size of the file descriptor cache (number of entries) |
| `-metadata-cache-size` | int  | 1073741824 | Maximum size of the metadata cache in bytes (1GB)     |

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
  -port 9000 \
  -http-port 9001 \
  -ttl 60 \
  -v
```

### Production Setup

Higher TTL, specific disk limits, optimized thresholds:

```bash
./ocache \
  -disk /var/cache/ocache \
  -port 9000 \
  -http-port 9001 \
  -ttl 3600 \
  -threshold 131072 \
  -segment-size 536870912 \
  -max-disk-usage 107374182400 \
  -fd-cache-size 5000
```

### Memory-Optimized Setup

Larger threshold for in-memory storage:

```bash
./ocache \
  -threshold 1048576 \
  -segment-size 1073741824 \
  -compact-threshold 67108864
```

### Disk-Optimized Setup

Smaller threshold, larger segments:

```bash
./ocache \
  -threshold 16384 \
  -segment-size 1073741824 \
  -max-disk-usage 1099511627776
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

## Performance Tuning

### Threshold Tuning

- **Lower threshold** (16KB-32KB): More disk usage, better for large object workloads
- **Default threshold** (64KB): Balanced performance
- **Higher threshold** (128KB-1MB): More memory usage, faster for small-medium objects

### Segment Size Tuning

- **Smaller segments** (64MB-128MB): More files, better for mixed workloads
- **Default segments** (256MB): Balanced file management
- **Larger segments** (512MB-1GB): Fewer files, better for large object workloads

### FD Cache Size

- Increase for workloads with many concurrent large object reads
- Default (1000) is suitable for most workloads
- Monitor file descriptor usage and adjust accordingly

### Compaction Threshold

- Controls when background compaction runs
- Lower values: More frequent compaction, less disk usage
- Higher values: Less CPU usage, more temporary disk usage

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
