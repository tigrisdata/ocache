# OCache

High-performance cache service with dual-storage architecture, supporting both gRPC and HTTP interfaces. Optimized for mixed workloads with intelligent routing between RocksDB (small objects) and disk storage (large objects).

## Features

- **Dual Storage**: RocksDB for small objects, segmented disk storage for large objects
- **Cluster Mode**: Distributed caching with consistent hashing across multiple nodes
- **Smart Routing**: Client-side routing with automatic topology discovery
- **Multiple Interfaces**: gRPC for high performance, HTTP REST for easy integration
- **TTL Support**: Automatic expiration with background cleanup
- **LRU Eviction**: Optional disk usage limits with automatic eviction
- **Connection Pooling**: Efficient resource utilization for high concurrency
- **High Performance**: Optimized for throughput and low latency

## Quick Start

### Single Node

```bash
# Clone and build
git clone https://github.com/tigrisdata/ocache.git
cd ocache
make install-deps
make all

# Run the server
./ocache -disk /tmp/cache -listen-addr :9000 -listen-http :9001

# Test with CLI
./ocachecli put mykey "hello world"
./ocachecli get mykey
```

### Cluster Mode

```bash
# Start a 3-node cluster
./ocache -cluster-enabled -node-id node1 -listen-addr :9001 \
  -cluster-addr :7001 -seeds "localhost:7002,localhost:7003"

./ocache -cluster-enabled -node-id node2 -listen-addr :9002 \
  -cluster-addr :7002 -seeds "localhost:7001,localhost:7003"

./ocache -cluster-enabled -node-id node3 -listen-addr :9003 \
  -cluster-addr :7003 -seeds "localhost:7001,localhost:7002"

# Use with cluster-aware client
./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" \
  put mykey "distributed value"
```

## Documentation

- [Installation Guide](docs/installation.md) - Build options and dependencies
- [Cluster Mode](docs/cluster.md) - Distributed caching setup and operations
- [Client Documentation](docs/client.md) - Go client library
- [CLI Client](docs/cli.md) - Command-line client usage
- [HTTP API Reference](docs/http_api.md) - HTTP REST API
- [Configuration](docs/configuration.md) - Server flags and tuning
- [Testing Guide](docs/testing.md) - Running and writing tests
- [Benchmark Guide](docs/benchmark.md) - Running benchmarks
- [Metrics Guide](docs/metrics.md) - Prometheus metrics
- [Static Builds](docs/static_build.md) - Building static binaries

## Basic Usage

### HTTP API

```bash
# Store data
curl -X POST "http://localhost:9001/v1/cache/mykey" \
  -d '{"data":"aGVsbG8gd29ybGQ=","ttl_seconds":3600}'

# Retrieve data
curl "http://localhost:9001/v1/cache/mykey"

# Delete data
curl -X DELETE "http://localhost:9001/v1/cache/mykey"
```

### CLI Client

```bash
# Store, retrieve, delete
./ocachecli put key1 "value1"
./ocachecli get key1
./ocachecli del key1

# Run benchmarks
./ocachecli bench --workload B --num-ops 100000
```

## Configuration

Key configuration flags:

| Flag                   | Default    | Description                                               |
| ---------------------- | ---------- | --------------------------------------------------------- |
| `-listen-addr`         | :9000      | gRPC server listen address                                |
| `-listen-http`         | :9001      | HTTP server listen address                                |
| `-disk`                | /var/cache | Storage directory                                         |
| `-ttl`                 | 0          | Default global TTL (seconds)                              |
| `-max-disk-usage`      | 0          | Max disk usage (0=unlimited)                              |
| `-metadata-cache-size` | 1073741824 | Maximum size of the metadata cache in bytes (1GB)         |
| `-cluster-enabled`     | false      | Enable cluster mode                                       |
| `-cluster-addr`        | :7000      | Cluster address                                           |
| `-seeds`               | ""         | Comma-separated list of seed nodes (required for cluster) |
| `-node-id`             | ""         | Unique node identifier (required for cluster)             |

See [Configuration Guide](docs/configuration.md) for complete options.

## Testing

Run tests with `make test` for unit tests or `make test-all` for the complete suite. You can run specific tests using `TEST=TestName` or `TESTRUN=Pattern` variables:

```bash
# Run all tests
make test-all

# Run specific test
make test TEST=TestCacheService_PutObjectAndGet

# Run tests matching pattern
make test-integration TESTRUN=Compaction
```

See [Testing Guide](docs/testing.md) for comprehensive testing documentation.
