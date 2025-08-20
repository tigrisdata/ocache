# Testing Guide

This guide covers how to run tests for OCache, including unit tests, integration tests, and end-to-end tests.

## Quick Start

```bash
# Run all unit tests
make test

# Run tests with race detector
make test-race

# Run integration tests
make test-integration

# Run all tests (unit + integration + e2e)
make test-all
```

## Test Suites

### Unit Tests

Fast, isolated tests for individual components:

```bash
make test              # Run all unit tests (server + client)
make test-server       # Server tests only
make test-client       # Client tests only
make test-race         # Run with race detector
make test-coverage     # Generate coverage report
```

### Integration Tests

Comprehensive storage layer tests:

```bash
make test-integration               # All integration tests (~5 minutes)
make test-integration-short         # Short mode (~30 seconds)
make test-integration-race          # With race detector
make test-integration-coverage      # With coverage report
```

Specialized integration test suites:

```bash
make test-integration-small         # Small objects (<64KB)
make test-integration-medium        # Medium objects (64KB-16MB)
make test-integration-large         # Large objects (>16MB)
make test-integration-compaction    # Compaction behavior
make test-integration-workflow      # Cross-component workflows
```

### End-to-End Tests

Full system tests using the actual gRPC API:

```bash
make test-e2e          # Shell-based E2E tests
```

## Running Individual Tests

You can run specific tests or test patterns using the `TEST` or `TESTRUN` variables:

### Using TEST (Exact Match)

Run a specific test by its exact name:

```bash
# Run a specific test function
make test TEST=TestCacheService_PutObjectAndGet

# Run a specific test in server
make test-server TEST=TestStorage

# Run a specific integration test
make test-integration TEST=TestIntegration_Compaction
```

### Using TESTRUN (Pattern Match)

Run all tests matching a pattern:

```bash
# Run all tests with "CacheService" in the name
make test TESTRUN=CacheService

# Run all storage-related tests
make test-server TESTRUN=Storage

# Run all compaction tests
make test-integration TESTRUN=Compaction
```

### With Specialized Targets

The TEST and TESTRUN variables work with all test targets:

```bash
# Override the default filter for specialized targets
make test-integration-small TEST=TestSmallObjects_Basic
make test-integration-medium TESTRUN=Concurrent

# Run specific tests with race detector
make test-race TEST=TestCacheService
make test-integration-race TESTRUN=Segment

# Generate coverage for specific tests
make test-coverage TEST=TestStorage
```

## Test Organization

### Directory Structure

```
ocache/
├── server/
│   └── *_test.go           # Server unit tests
├── client/
│   └── *_test.go           # Client unit tests
├── tests/
│   ├── integration/        # Integration test suite
│   │   └── *_test.go
│   └── e2e/               # End-to-end test scripts
│       └── *.sh
```

### Test Categories

- **Unit Tests**: Test individual functions and components in isolation
- **Integration Tests**: Test the storage layer directly without gRPC
- **E2E Tests**: Test the complete system through its public APIs

## Environment Variables

Some test behaviors can be controlled via environment variables:

```bash
# Override cleanup interval for tests
OCACHE_TEST_CLEANUP_INTERVAL=1s make test-integration

# Run integration tests in short mode
go test -short ./tests/integration/...
```

## Continuous Integration

For CI environments, use these targets:

```bash
# Run all checks (formatting, vetting, tests)
make check

# Lint without modifying files
make lint-ci

# Run full test suite
make test-all
```

## Troubleshooting

### Common Issues

1. **RocksDB not found**: Install dependencies first

   ```bash
   make install-deps
   make install-rocksdb
   ```

2. **Tests timeout**: Increase timeout for slower systems

   ```bash
   # Edit Makefile to increase timeout values
   # Default is 60s for unit tests, 300s for integration
   ```

3. **Race detector slowdown**: Race detection significantly slows tests
   ```bash
   # Use regular tests for quick feedback
   make test
   # Use race detector before committing
   make test-race
   ```

### Debugging Tests

Run tests with verbose output:

```bash
# Verbose output is already enabled by default in Makefile
make test-server

# For even more detail, run go test directly
cd server && go test -v -run TestCacheService ./...
```

## Best Practices

1. **Run tests before committing**: Use `make test` for quick validation
2. **Check race conditions**: Run `make test-race` before PRs
3. **Verify integration**: Run `make test-integration-short` for storage changes
4. **Use specific tests during development**: Use TEST/TESTRUN for faster feedback
5. **Clean test artifacts**: Use `make clean` to remove test files

## Coverage Reports

Generate and view test coverage:

```bash
# Generate HTML coverage report
make test-coverage

# View the report
open coverage.html

# Generate integration test coverage
make test-integration-coverage
open coverage-integration.html
```

Coverage reports show which code paths are tested and help identify gaps in test coverage.
