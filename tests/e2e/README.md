# E2E Tests

This directory contains comprehensive end-to-end (e2e) tests for the OCache service.

## Test Suite

The e2e test suite covers all major functionality of OCache:

### 1. Concurrent Operations Test (`concurrent_ops_test.sh`)

- Tests concurrent read, write, and delete operations
- Verifies data consistency under concurrent workload
- Tests read-after-write consistency
- Validates mixed operations (puts, gets, deletes) running simultaneously

### 2. Storage Layers Test (`storage_layers_test.sh`)

- Tests reads from different storage layers:
  - RocksDB inline storage (small objects <64KB)
  - Raw files (medium objects 64KB-16MB)
  - Compacted segments
- Verifies data integrity across storage transitions
- Tests large value streaming

### 3. TTL Cleaner Test (`ttl_cleaner_test.sh`)

- Tests TTL expiration functionality
- Verifies background TTL cleaner operation
- Tests TTL with different storage layers
- Validates TTL update on key overwrite
- Tests concurrent operations with TTL

### 4. LRU Eviction Test (`lru_eviction_test.sh`)

- Tests LRU eviction when disk usage limit is reached
- Verifies recently accessed keys are retained
- Tests eviction with mixed object sizes
- Validates LRU behavior under continuous load
- Tests interaction between LRU and TTL

### 5. Compaction Test (`compaction_test.sh`)

- Tests compaction from raw files to segments
- Verifies API operations work correctly during compaction
- Tests mixed operations while compaction is active
- Validates data integrity after server restart with compacted segments

### 6. Recompaction Test (`recompaction_test.sh`)

- Tests recompaction for segment defragmentation
- Verifies defragmentation after deletes
- Tests multiple recompaction cycles
- Validates concurrent operations during recompaction
- Tests heavy fragmentation scenarios (>50% deleted keys)

## Running the Tests

```bash
# Run all e2e tests
make test-e2e

# Run individual test suites
make test-e2e-concurrent      # Concurrent operations test
make test-e2e-storage-layers  # Storage layers test
make test-e2e-ttl             # TTL functionality test
make test-e2e-lru             # LRU eviction test
make test-e2e-compaction      # Compaction test
make test-e2e-recompaction    # Recompaction test
make test-e2e-legacy          # Legacy TTL/LRU test

# Or run test scripts directly
./tests/e2e/concurrent_ops_test.sh
./tests/e2e/storage_layers_test.sh
./tests/e2e/ttl_cleaner_test.sh
./tests/e2e/lru_eviction_test.sh
./tests/e2e/compaction_test.sh
./tests/e2e/recompaction_test.sh
```

## Requirements

- Built `ocache` and `ocachecli` binaries in the project root
- Local filesystem access for temporary test data
- Sufficient disk space for test operations

## Test Output

Each test provides colored output:

- 🟢 Green: Test passed
- 🔴 Red: Test failed
- 🟡 Yellow: Warning or informational

Tests exit with:

- Exit code 0: All tests passed
- Exit code 1: One or more tests failed

## Test Data

Tests use temporary directories under `/tmp/ocache-*`:

- `/tmp/ocache-concurrent-test`
- `/tmp/ocache-storage-test`
- `/tmp/ocache-ttl-test`
- `/tmp/ocache-lru-test`
- `/tmp/ocache-compaction-test`
- `/tmp/ocache-recompaction-test`

These directories are automatically cleaned up after each test run.

## Adding New Tests

When adding new e2e tests:

1. Create executable shell scripts in this directory
2. Follow the naming convention: `<feature>_test.sh`
3. Use colored output for clear test results
4. Ensure proper cleanup of test data
5. Add a corresponding Makefile target
6. Update this README with test description
