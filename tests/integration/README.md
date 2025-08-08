# Integration Tests

This directory contains integration tests for OCache that verify the storage layer components and their interactions. These tests directly test the storage internals including RocksDB storage, raw file handling, segment compaction, and background processes.

**Note**: These are integration tests that directly test the storage layer. For true end-to-end tests using the gRPC API and cache client, see the shell scripts in the e2e tests directory.

## Test Categories

### 1. Small Object Tests (< 64KB)

Tests for objects stored inline in RocksDB.

**Test Coverage:**

- Basic CRUD operations
- TTL expiration
- LRU eviction
- Concurrent access
- Edge cases (empty values, binary data, unicode, etc.)
- Update operations

**Files:**

- `small_objects_test.go` - Test implementations

### 2. Medium Object Tests (64KB - 16MB)

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

**Files:**

- `medium_objects_test.go` - Test implementations

### 3. Large Object Tests (> 16MB)

_To be implemented_

Tests for objects stored as permanent raw files (never compacted).

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

# Run only small object tests
make test-integration-small

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

## Environment Variables

- `OCACHE_TEST_CLEANUP_INTERVAL`: Override cleanup interval for tests

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
