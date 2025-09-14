# OCache

High-performance cache service with dual-storage architecture, supporting both gRPC and HTTP interfaces. Optimized for mixed workloads with intelligent routing between RocksDB (small objects) and disk storage (large objects).

## Features

- **Dual Storage**: RocksDB for small objects, segmented disk storage for large objects
- **Multiple Interfaces**: gRPC for high performance, HTTP REST for easy integration
- **TTL Support**: Automatic expiration with background cleanup
- **LRU Eviction**: Optional disk usage limits with automatic eviction
- **High Performance**: Optimized for throughput and low latency

## Quick Start

```bash
# Clone and build
git clone https://github.com/tigrisdata/ocache.git
cd ocache
make install-deps
make all

# Run the server
./ocache -disk /tmp/cache -port 9000 -http-port 9001

# Test with CLI
./ocachecli put mykey "hello world"
./ocachecli get mykey
```

## Documentation

- [Installation Guide](docs/installation.md) - Build options and dependencies
- [Client Documentation](docs/client.md) - Go client library and CLI
- [HTTP API Reference](docs/http_api.md) - HTTP REST API
- [CLI Client](docs/cli.md) - Command-line client usage
- [Configuration](docs/configuration.md) - Server flags and tuning
- [Testing Guide](docs/testing.md) - Running and writing tests
- [Static Builds](docs/static_build.md) - Production deployment
- [Benchmark Guide](docs/benchmark.md) - Running benchmarks
- [Metrics Guide](docs/metrics.md) - Prometheus metrics

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

| Flag                   | Default    | Description                                       |
| ---------------------- | ---------- | ------------------------------------------------- |
| `-port`                | 9000       | gRPC port                                         |
| `-http-port`           | 9001       | HTTP port                                         |
| `-disk`                | /var/cache | Storage directory                                 |
| `-ttl`                 | 0          | Default global TTL (seconds)                      |
| `-max-disk-usage`      | 0          | Max disk usage (0=unlimited)                      |
| `-metadata-cache-size` | 1073741824 | Maximum size of the metadata cache in bytes (1GB) |

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
