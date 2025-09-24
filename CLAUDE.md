# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OCache is a high-performance cache service with dual-storage architecture, supporting both gRPC and HTTP interfaces. It's optimized for mixed workloads with intelligent routing between RocksDB (small objects) and disk storage (large objects).

## Build Commands

**Installation and Setup:**

```bash
make install-deps          # Install protoc and plugins
make install-rocksdb       # Install RocksDB (via brew/apt)
make install-rocksdb-from-source  # Build RocksDB from source
```

**Building:**

```bash
make all                   # Build both server (ocache) and CLI (ocachecli)
make build                 # Build server only
make build-static          # Build server with static RocksDB
make build-cli             # Build CLI client only
make proto                 # Generate protobuf code
```

**Development:**

```bash
make run                   # Run server with default settings
make run-verbose           # Run with debug logging
make bench                 # Run benchmarks
```

## Testing Commands

**IMPORTANT:** Always use `make test*` targets to run tests, never run `go test` directly. The Makefile handles proper CGO flags, timeouts, and other platform-specific configurations.

**Unit Tests:**

```bash
make test                  # Run all unit tests (server + client)
make test-server           # Server tests only
make test-storage          # Storage tests only
make test-client           # Client tests only
make test-race             # Unit tests with race detector
make test-coverage         # Generate coverage report
```

**Integration Tests:**

```bash
make test-integration               # All integration tests (~5+ minutes)
make test-integration-short         # Short mode (~30s)
make test-integration-small         # Small objects only (~10s)
make test-integration-medium        # Medium objects (~60s)
make test-integration-large         # Large objects (~120s)
make test-integration-compaction     # Compaction
make test-integration-cleaner       # TTL and LRU cleaner
make test-integration-workflow      # Cross-component workflow
make test-integration-race          # With race detector
make test-integration-coverage      # With coverage
```

**End-to-End Tests:**

```bash
make test-e2e              # Shell-based E2E tests
make test-all              # All tests (unit + integration + e2e)
```

**Running Individual Tests:**

You can run specific tests using `TEST` (exact match) or `TESTRUN` (pattern match) variables:

```bash
# Run specific test by exact name
make test TEST=TestCacheService_PutObjectAndGet
make test-storage TEST=TestStorage

# Run tests matching a pattern
make test TESTRUN=CacheService
make test-integration TESTRUN=Compaction

# Works with all test targets
make test-integration-small TEST=TestSmallObjects_Basic
make test-race TESTRUN=Storage
```

## Code Quality

```bash
make lint                  # Run all linters (vet, fmt check, mod tidy)
make lint-fix              # Fix formatting issues
make vet                   # Run go vet
make fmt                   # Format code
make check                 # All checks (fmt, vet, test)
```

## Architecture

**Multi-Module Structure:**

- Uses Go workspace (`go.work`) with 4 modules:
  - `server/` - Main cache server with gRPC/HTTP APIs
  - `client/` - CLI client and Go client library
  - `proto/` - Protocol buffer definitions
  - `tests/integration/` - Integration test suite

**Cluster Architecture:**

- **Distributed Mode:**

  - Consistent hashing with 16384 partitions
  - Coordinator service for membership management
  - Smart client routing with topology caching
  - Connection pooling for high throughput
  - Heartbeat-based failure detection

- **Key Cluster Components:**
  - `coordinator/` - Cluster coordinator and topology management
  - `coordinator/proto/` - Cluster service protobuf definitions
  - `common/hash/` - Consistent hashing implementation
  - `client/cluster_client.go` - Cluster-aware client
  - `client/simple_client.go` - Direct connection client

**Storage Architecture:**

- **Dual Storage Strategy:**
  - Small objects (< 64KB): Stored in RocksDB as `ValueType_INLINE`
  - Medium objects (64KB - 16MB): Initially raw files, eligible for compaction to segments
  - Large objects (> 16MB): Permanent raw files, never compacted

**Key Storage Components:**

- `storage/storage.go` - Main storage interface and coordination
- `storage/segment/` - Segment-based storage for compacted medium objects
- `storage/files/` - File manager for raw file operations
- `storage/fd/` - File descriptor caching
- `storage/bufferpool/` - Memory buffer management
- `compaction/compactor.go` - Background compaction service
- `storage/cleaner.go` - TTL cleanup service

**API Layers:**

- gRPC API with streaming support for large objects
- HTTP REST API via grpc-gateway
- CLI client with benchmarking capabilities
- Cluster topology service for node discovery

## Development Notes

**RocksDB Integration:**

- Requires CGO with RocksDB headers/libraries
- Makefile handles platform-specific CGO flags
- Uses `linxGnu/grocksdb` Go bindings

**Testing Strategy:**

- Always use `make test*` targets to run tests (handles CGO flags and platform-specific settings)
- Never run `go test` directly - the Makefile ensures proper configuration
- Unit tests focus on individual components
- Integration tests verify storage layer directly (not via gRPC)
- E2E tests use actual gRPC API calls
- Tests avoid manual cleanup to prevent deadlocks with background processes
- Use `TEST=TestName` or `TESTRUN=Pattern` to run specific tests during development

**Background Processes:**

- Compactor: Migrates raw files to segments
- Cleaner: Handles TTL expiration
- Access Index: Updates LRU access patterns
- Coordinator: Manages cluster membership (cluster mode)
- All coordinate via storage layer interfaces

**Configuration:**

- Command-line flags only (no config files)
- Key single-node flags: `-disk`, `-threshold`, `-ttl`, `-max-disk-usage`
- Key cluster flags: `-cluster-enabled`, `-node-id`, `-cluster-addr`, `-seeds`
- See `docs/configuration.md` for complete options

**Cluster Testing:**

To test cluster functionality:

```bash
# Start 3-node cluster locally
./ocache -cluster-enabled -node-id node1 -listen-addr :9001 -cluster-addr :7001 -seeds localhost:7002,localhost:7003 &
./ocache -cluster-enabled -node-id node2 -listen-addr :9002 -cluster-addr :7002 -seeds localhost:7001,localhost:7003 &
./ocache -cluster-enabled -node-id node3 -listen-addr :9003 -cluster-addr :7003 -seeds localhost:7001,localhost:7002 &

# Test with cluster-aware client
./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" put test "value"
```

## Important Development Patterns

**Code Formatting**

- Format the code and fix linting issues before every code commit
