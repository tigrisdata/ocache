# Integration Test Conventions

## Test Organization

### Test Suites
- **ObjectsSuite**: Core object operations (put, get, delete) across all sizes
- **CleanerSuite**: TTL expiration and LRU eviction
- **CompactionSuite**: Segment compaction and recompaction
- **WorkflowSuite**: Cross-component workflows and coordination
- **StressSuite**: High-load and concurrent operation tests

### Test Files
- `objects_test.go`: Object size-specific operations
- `cleaner_test.go`: Cleanup and eviction logic
- `compaction_test.go`: Compaction and segment management
- `workflow_test.go`: End-to-end workflows
- `parameterized_helpers.go`: Reusable parameterized test utilities
- `test_patterns.go`: Common test patterns and scenarios
- `test_utils.go`: Core test harness and utilities
- `verification.go`: Verification and assertion helpers
- `data_generators.go`: Test data generation utilities

## Naming Conventions

### Test Function Names
Format: `Test_<Component>_<Scenario>`

Examples:
- `Test_Objects_BasicFlow`
- `Test_Cleaner_TTLExpiration`
- `Test_Compaction_AutoTrigger`
- `Test_Workflow_MixedSizes`

### Test Case Names
Format: `<category>-<specifics>`

Examples:
- `small-1KB`
- `medium-boundary`
- `large-streaming`
- `ttl-short-lived`
- `lru-old-object`

### Key Naming
Format: `<test>-<category>-<identifier>`

Examples:
- `object-small-1`
- `compact-medium-batch1`
- `ttl-expire-2s`
- `lru-evict-old`
- `workflow-mixed-5`

## Test Categories

### By Object Size
- **Small**: 0 - 64KB (inline storage in RocksDB)
- **Medium**: 64KB - 16MB (raw files, eligible for compaction)
- **Large**: > 16MB (permanent raw files)

### By Operation Type
- **Basic**: Standard CRUD operations
- **Concurrent**: Parallel operations
- **Streaming**: Chunked read/write
- **Mixed**: Combined operation types

### By Feature
- **TTL**: Time-to-live expiration
- **LRU**: Least recently used eviction
- **Compaction**: Segment creation and management
- **Recovery**: Error handling and recovery

## Test Patterns

### Parameterized Tests
Use helper functions from `parameterized_helpers.go`:
- `RunObjectSizeTests`: Test across object sizes
- `RunTTLTests`: Test TTL expiration scenarios
- `RunLRUTests`: Test LRU eviction scenarios
- `RunConcurrentTest`: Test concurrent operations
- `RunUpdateTests`: Test update operations

### Test Patterns
Use patterns from `test_patterns.go`:
- `MixedWorkloadPattern`: Mixed read/write/delete
- `CompactionPattern`: Compaction behavior
- `TTLExpirationPattern`: TTL expiry
- `StreamingPattern`: Streaming operations

## Best Practices

### Test Isolation
- Each test should clean up after itself
- Use unique key prefixes per test
- Don't rely on test execution order

### Performance
- Use parameterized tests to reduce duplication
- Batch similar test scenarios
- Reuse test harness when possible

### Verification
- Always verify data integrity
- Check both positive and negative cases
- Verify background process effects

### Documentation
- Use descriptive test names
- Add comments for complex scenarios
- Document expected behavior

## Common Utilities

### Data Generation
- `GenerateRandomData(size)`: Random bytes
- `GenerateSequentialData(size)`: Sequential pattern
- `GenerateCompressibleData(size)`: Highly compressible

### Verification
- `VerifyDataIntegrity()`: Check data correctness
- `VerifyStorageType()`: Check storage location
- `VerifySegmentsExist()`: Check segment creation
- `VerifyTTLCleanup()`: Check TTL expiration
- `VerifyLRUEviction()`: Check LRU behavior

### Test Harness
- `NewIntegrationTestHarness()`: Create test environment
- `PutObject()`: Store object
- `GetObject()`: Retrieve object
- `DeleteObject()`: Remove object
- `SetAccessTime()`: Set LRU access time
- `FlushAccessUpdates()`: Force access index update

## Running Tests

### All Integration Tests
```bash
make test-integration
```

### Specific Test Suites
```bash
make test-integration TEST=TestObjectsSuite
make test-integration TEST=TestCleanerSuite
make test-integration TEST=TestCompactionSuite
```

### Individual Tests
```bash
make test-integration TESTRUN=BasicFlow
make test-integration TESTRUN=TTLExpiration
```

### With Options
```bash
make test-integration-short    # Quick tests only
make test-integration-race     # With race detector
make test-integration-coverage # With coverage report
```