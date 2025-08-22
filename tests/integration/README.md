# Integration Tests

This directory contains integration tests for OCache that verify the storage layer components and their interactions. These tests directly test the storage internals including RocksDB storage, raw file handling, segment compaction, and background processes.

**Note**: These are integration tests that directly test the storage layer. For true end-to-end tests using the gRPC API and cache client, see the shell scripts in the e2e tests directory.

## Quick Start

```bash
# Run all implemented integration tests
make test-integration

# Run a specific category (fastest to slowest)
make test-integration-small   # ~10s
make test-integration-medium  # ~60s
make test-integration-large   # ~120s

# Run with verbose output to see test progress
cd tests/integration
CGO_CFLAGS="-I/opt/homebrew/include" CGO_LDFLAGS="-L/opt/homebrew/lib -lrocksdb -lstdc++ -lz -lbz2 -lsnappy -llz4 -lzstd" \
  go test -v ./...
```

## Test Categories

### 1. Small Object Tests (< 64KB) ✅

Tests for objects stored inline in RocksDB.

**Test Coverage:**

- Basic CRUD operations
- TTL expiration
- LRU eviction
- Concurrent access
- Edge cases (empty values, binary data, unicode, etc.)
- Update operations

**Key Tests:**

- `Test_SmallObject_BasicFlow` - Basic storage and retrieval
- `Test_SmallObject_TTLExpiration` - TTL functionality
- `Test_SmallObject_LRUEviction` - LRU eviction behavior
- `Test_SmallObject_ConcurrentAccess` - Thread safety
- `Test_SmallObject_EdgeCases` - Boundary conditions
- `Test_SmallObject_UpdateOperations` - Overwrite scenarios

**Files:**

- `small_objects_test.go` - Test implementations

### 2. Medium Object Tests (64KB - 16MB) ✅

Tests for objects stored as raw files and eligible for compaction.

**Test Coverage:**

- Raw file creation and storage
- Compaction flow from raw files to segments
- Partial compaction scenarios
- Concurrent operations
- Update operations
- TTL functionality
- Edge cases and boundary conditions
- Streaming writes
- Compaction with TTL objects

**Key Tests:**

- `Test_MediumObject_RawFileCreation` - Raw file storage
- `Test_MediumObject_CompactionFlow` - Raw to segment migration
- `Test_MediumObject_PartialCompaction` - Mixed compaction scenarios
- `Test_MediumObject_Concurrent` - Concurrent access patterns
- `Test_MediumObject_EdgeCases` - Boundary sizes and patterns
- `Test_MediumObject_TTL` - TTL with medium objects
- `Test_MediumObject_StreamingWrite` - Streaming operations
- `Test_MediumObject_CompactionWithTTL` - Compaction + TTL interaction

**Files:**

- `medium_objects_test.go` - Test implementations

### 3. Large Object Tests (> 16MB) ✅

Tests for objects stored as permanent raw files (never compacted).

**Test Coverage:**

- Permanent raw file storage
- Exclusion from compaction
- Streaming read operations
- Concurrent access to large files
- TTL for large objects
- Update operations
- Various sizes (17MB to 256MB+)

**Key Tests:**

- `Test_LargeObject_PermanentRawFile` - Verifies no compaction
- `Test_LargeObject_CompactionExclusion` - Mixed size compaction
- `Test_LargeObject_Streaming` - Chunked reading & concurrent access
- `Test_LargeObject_MixedSizes` - Various large sizes
- `Test_LargeObject_TTL` - TTL with large objects
- `Test_LargeObject_Updates` - Updating large objects

**Files:**

- `large_objects_test.go` - Test implementations

### 4. Compaction Tests

_To be implemented_

Tests for the background compaction process.

### 5. Workflow Tests

_To be implemented_

Tests for cross-component interactions.

### 6. Stress Tests

_To be implemented_

Tests for system behavior under load.

## Running the Tests

### Using Makefile Targets (Recommended)

The easiest way to run the integration tests is using the provided Makefile targets, which handle all the necessary CGO configuration for RocksDB:

```bash
# Run all integration tests
make test-integration

# Run integration tests in short mode (faster)
make test-integration-short

# Run specific test categories
make test-integration-small   # Small object tests (< 64KB)
make test-integration-medium  # Medium object tests (64KB - 16MB)
make test-integration-large   # Large object tests (> 16MB)

# Run with race detector
make test-integration-race

# Run with coverage report
make test-integration-coverage

# Run all tests (unit + integration)
make test-all
```

### Manual Execution

If running tests manually, you need to set CGO flags for RocksDB:

```bash
# macOS with Homebrew
export CGO_CFLAGS="-I/opt/homebrew/include"
export CGO_LDFLAGS="-L/opt/homebrew/lib"

# Linux (adjust paths as needed)
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L/usr/lib -L/usr/local/lib"

# Run all integration tests
cd tests/integration
go test -v ./...

# Run specific test suite
go test -v -run TestIntegration_SmallObjects

# Run specific test
go test -v -run TestIntegration_SmallObjects/SmallObjectSuite/Test_SmallObject_BasicFlow

# Run with race detection
go test -v -race -run TestIntegration_SmallObjects

# Run with coverage
go test -v -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Test Infrastructure

### Core Files

- **test_utils.go**: Test harness and utilities

  - `IntegrationTestHarness`: Main test orchestrator
  - `IntegrationTestConfig`: Test configuration
  - Storage operations helpers
  - Metrics collection

- **data_generators.go**: Test data generation

  - Size-based generators (small/medium/large)
  - Special data patterns (binary, unicode, compressible)
  - Edge case data generation

- **verification.go**: Verification utilities

  - Storage type verification
  - Data integrity checks
  - Disk usage verification
  - Compaction verification

- **integration_runner.go**: Test suite framework
  - Base test suites
  - Setup/teardown logic
  - Suite runners

## Test Configuration

Tests can be configured through `IntegrationTestConfig`:

```go
type IntegrationTestConfig struct {
    InlineThreshold    int64         // Default: 64KB
    CompactThreshold   int64         // Default: 16MB
    SegmentSize        int64         // Default: 256MB
    CompactionInterval time.Duration // Default: 1s (for tests)
    CleanupInterval    time.Duration // Default: 1s (for tests)
    MaxDiskUsage       int64         // Default: 0 (no limit)
    FDCacheSize        int           // Default: 100
}
```

## Test Patterns

### Basic Test Flow

```go
func (s *TestSuite) TestExample() {
    // 1. Generate test data
    data := GenerateRandomData(1024)

    // 2. Perform operation
    err := s.Harness.PutObject("key", data, 0)
    require.NoError(s.T(), err)

    // 3. Verify storage type
    VerifyStorageType(s.T(), s.Harness.TempDir, "key", pb.ValueType_INLINE)

    // 4. Verify data integrity
    retrieved, err := s.Harness.GetObject("key")
    require.NoError(s.T(), err)
    VerifyDataIntegrity(s.T(), data, retrieved)

    // 5. Clean up
    err = s.Harness.DeleteObject("key")
    require.NoError(s.T(), err)
}
```

### Concurrent Test Pattern

```go
var wg sync.WaitGroup
errors := make(chan error, numGoroutines)

wg.Add(numGoroutines)
for i := 0; i < numGoroutines; i++ {
    go func(id int) {
        defer wg.Done()
        // Perform concurrent operations
        if err := operation(); err != nil {
            errors <- err
        }
    }(i)
}

wg.Wait()
close(errors)

// Check for errors
for err := range errors {
    require.NoError(s.T(), err)
}
```

## Debugging Tips

1. **Enable verbose logging**: Use `-v` flag
2. **Run single test**: Use `-run` with specific test name
3. **Disable test caching**: Use `-count=1`
4. **Check temp directories**: Tests create temp dirs with pattern `ocache-integration-test-*`
5. **View metrics**: Tests print metrics after completion

## Important Implementation Notes

### Architectural Considerations

1. **TTL and Cleaner Coordination**: The storage layer's `Get` method no longer deletes expired keys directly. This responsibility is delegated entirely to the background Cleaner to avoid race conditions and deadlocks.

2. **File Descriptor Management**: All test code properly closes `io.ReadCloser` instances returned by `storage.Get()` to prevent file descriptor leaks, especially important for large object tests.

3. **Manual Cleanup Avoided**: Tests skip manual cleanup (`DeleteObject`) at the end to avoid deadlocks with background processes. The test harness teardown handles all cleanup.

### Storage Behavior

- **Small Objects (< 64KB)**: Stored inline in RocksDB as `ValueType_INLINE`
- **Medium Objects (64KB - 16MB)**: Initially stored as `ValueType_RAW_FILE`, eligible for compaction to segments
- **Large Objects (> 16MB)**: Permanently stored as `ValueType_RAW_FILE`, never compacted

### Known Limitations

- `VerifyNoCompactionEntry` currently only logs verification intent due to limited access to internal RocksDB compaction keys
- Direct RocksDB verification is skipped for raw file and segment types to avoid implementation coupling

## Adding New Tests

1. Choose appropriate test suite (SmallObjectSuite, MediumObjectSuite, etc.)
2. Follow naming convention: `Test_<Category>_<Feature>`
3. Use provided utilities for common operations
4. Always verify storage type and data integrity
5. Clean up resources in defer or teardown

## CI Integration

These tests can be integrated into CI with:

```yaml
# Run small object tests (fast)
- run: go test -v ./tests/integration -run TestIntegration_SmallObjects

# Run all tests (slower)
- run: go test -v ./tests/integration -all

# Run with race detection
- run: go test -race ./tests/integration -run TestIntegration_SmallObjects
```
