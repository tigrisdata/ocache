# Integration Test Plan for OCache

## Overview
This document outlines a comprehensive integration testing strategy for OCache's storage layer, covering the complete data flow from initial storage through compaction and cleanup processes. The tests are designed to validate the interaction between RocksDB metadata storage, raw file handling, segment compaction, and background cleanup processes.

**Note**: These integration tests directly test the storage layer components. For true end-to-end tests that exercise the full stack including the gRPC API, see the E2E test scripts that use the cache client.

## Test Categories

### 1. Small Object Tests (< 64KB - Inline Storage)
**Purpose**: Validate that small objects are stored inline in RocksDB without using external files.

#### Test Scenarios:
- **Test_SmallObject_BasicFlow**
  - Store objects of varying sizes: 1B, 1KB, 32KB, 63KB, 64KB (exact threshold)
  - Verify objects are stored with ValueType_INLINE
  - Confirm no raw files or segments are created
  - Validate retrieval returns exact data
  - Test TTL expiration for inline objects
  - Verify LRU eviction for inline objects

- **Test_SmallObject_Concurrent**
  - Concurrent writes of 1000 small objects (random sizes 1B-63KB)
  - Concurrent reads during writes
  - Verify data integrity and correct storage type
  - Monitor memory usage stays within limits

- **Test_SmallObject_EdgeCases**
  - Exactly 64KB object (boundary condition)
  - Empty value (0 bytes)
  - Maximum key length scenarios
  - Unicode and binary data handling

### 2. Medium Object Tests (64KB - 16MB - Raw Files)
**Purpose**: Validate raw file storage and compaction for medium-sized objects.

#### Test Scenarios:
- **Test_MediumObject_RawFileCreation**
  - Store objects: 65KB, 100KB, 1MB, 8MB, 15MB, 16MB (boundary)
  - Verify ValueType_RAW_FILE storage
  - Confirm raw files exist in storage directory
  - Validate file content matches stored data
  - Check compaction index entries created

- **Test_MediumObject_CompactionFlow**
  - Store 100 medium objects (100KB - 1MB each)
  - Trigger compaction manually
  - Verify objects migrate from RAW_FILE to SEGMENT
  - Confirm raw files are deleted post-compaction
  - Validate segment files contain correct data
  - Test reading during compaction (no disruption)

- **Test_MediumObject_PartialCompaction**
  - Store mix of compactable and non-compactable files
  - Some files deleted before compaction
  - Some files updated during compaction
  - Verify only valid files are compacted
  - Test compaction recovery from errors

### 3. Large Object Tests (> 16MB - Permanent Raw Files)
**Purpose**: Validate that large objects are stored as raw files and never compacted.

#### Test Scenarios:
- **Test_LargeObject_PermanentRawFile**
  - Store objects: 17MB, 50MB, 100MB, 200MB
  - Verify ValueType_RAW_FILE storage
  - Confirm raw files exist and persist
  - Verify NO compaction index entries created (no !compact/ prefix)
  - Confirm files never migrate to segments

- **Test_LargeObject_CompactionExclusion**
  - Store mix of medium (1MB-15MB) and large (20MB+) objects
  - Trigger compaction multiple times
  - Verify only medium objects get compacted to segments
  - Confirm large objects remain as RAW_FILE permanently
  - Validate large object raw files are never deleted

- **Test_LargeObject_Streaming**
  - Stream large objects (100MB+) from raw files
  - Verify efficient chunked reading from raw files
  - Test concurrent reads from same raw file
  - Validate checksum on full read
  - Test file descriptor caching for large raw files

### 4. Compaction Loop Integration Tests
**Purpose**: Validate the background compaction process and its interaction with other operations.

#### Test Scenarios:
- **Test_CompactionLoop_AutoTrigger**
  - Configure short compaction interval (1 second)
  - Store 50 medium objects
  - Wait for automatic compaction
  - Verify all eligible files compacted
  - Monitor compaction metrics

- **Test_CompactionLoop_ConcurrentOperations**
  - Run compaction while:
    - Writing new objects
    - Reading existing objects
    - Deleting objects
    - Updating objects
  - Verify no data corruption or loss
  - Check operation latencies remain acceptable

- **Test_CompactionLoop_SegmentManagement**
  - Fill multiple segments via compaction
  - Verify segment rotation and finalization
  - Test segment cleanup for deleted objects
  - Validate disk space reclamation

- **Test_CompactionLoop_ErrorRecovery**
  - Simulate disk full during compaction
  - Simulate segment write failures
  - Kill process during compaction
  - Verify recovery on restart
  - Check data integrity post-recovery

### 5. Cross-Component Workflow Tests
**Purpose**: Test complete workflows involving multiple components working together.

#### Test Scenarios:
- **Test_Workflow_MixedObjectSizes**
  - Store mix: 100 small, 50 medium, 10 large objects
  - Run compaction and cleanup cycles
  - Perform random reads/updates/deletes
  - Verify storage distribution correctness
  - Monitor resource usage (memory, disk, FDs)

- **Test_Workflow_TTLAndLRU**
  - Store objects with varying TTLs
  - Set max disk usage for LRU activation
  - Fill cache to trigger LRU eviction
  - Verify TTL cleanup runs correctly
  - Confirm LRU evicts oldest accessed items
  - Test interaction between TTL and LRU

- **Test_Workflow_BackgroundProcessCoordination**
  - Run all background processes simultaneously:
    - Compaction loop
    - TTL cleanup
    - LRU eviction
    - Access time updates
  - Verify no deadlocks or race conditions
  - Check process isolation and independence

- **Test_Workflow_CacheWarming**
  - Start with populated cache (mixed object types)
  - Restart server
  - Verify metadata reconstruction
  - Test immediate availability of all data
  - Validate background processes resume correctly

### 6. Stress and Performance Tests
**Purpose**: Validate system behavior under load and edge conditions.

#### Test Scenarios:
- **Test_Stress_HighThroughput**
  - 10,000 writes/second of mixed sizes
  - 50,000 reads/second concurrent
  - Measure latencies (p50, p95, p99)
  - Monitor resource usage trends
  - Verify no memory leaks

- **Test_Stress_LargeDataset**
  - Store 100GB of data (mixed sizes)
  - Verify disk usage tracking accuracy
  - Test compaction efficiency at scale
  - Measure cleanup performance

- **Test_Stress_FileDescriptorLimits**
  - Set low FD cache size (10 FDs)
  - Access 1000 different raw files and segments
  - Verify FD cache eviction works
  - Check no FD leaks

- **Test_Stress_ConcurrentCompaction**
  - Multiple compaction workers (if supported)
  - Verify segment locking prevents conflicts
  - Test compaction throughput scaling

## Implementation Strategy

### Test Infrastructure

```go
// test_utils.go
type IntegrationTestConfig struct {
    InlineThreshold    int64
    CompactThreshold   int64
    SegmentSize        int64
    CompactionInterval time.Duration
    CleanupInterval    time.Duration
    MaxDiskUsage       int64
}

type IntegrationTestHarness struct {
    Storage    *Storage
    Config     IntegrationTestConfig
    TempDir    string
    Metrics    *TestMetrics
    Cleanup    func()
}

func NewIntegrationTestHarness(t *testing.T, config IntegrationTestConfig) *IntegrationTestHarness {
    // Setup isolated test environment
    // Initialize storage with config
    // Start metric collection
}

// Helper methods
func (h *E2ETestHarness) GenerateObject(size int64) []byte
func (h *E2ETestHarness) WaitForCompaction(timeout time.Duration) error
func (h *E2ETestHarness) VerifyStorageType(key string, expected ValueType)
func (h *E2ETestHarness) GetStorageStats() StorageStats
```

### Test Data Generators

```go
// data_generators.go
func GenerateSmallObjects(count int) []TestObject    // 1B - 64KB
func GenerateMediumObjects(count int) []TestObject   // 64KB - 16MB  
func GenerateLargeObjects(count int) []TestObject    // 16MB - 256MB
func GenerateMixedObjects(small, medium, large int) []TestObject

type TestObject struct {
    Key      string
    Data     []byte
    Size     int64
    Checksum uint32
    TTL      *time.Duration
}
```

### Verification Utilities

```go
// verification.go
func VerifyNoRawFiles(t *testing.T, storageDir string)
func VerifySegmentIntegrity(t *testing.T, segmentPath string)
func VerifyCompactionComplete(t *testing.T, storage *Storage)
func VerifyDataIntegrity(t *testing.T, original, retrieved []byte)
func VerifyMetrics(t *testing.T, expected, actual *TestMetrics)
```

### Test Execution Framework

```go
// integration_runner.go
type IntegrationTestSuite struct {
    suite.Suite
    Harness *IntegrationTestHarness
}

func (s *E2ETestSuite) SetupTest() {
    // Per-test setup
}

func (s *E2ETestSuite) TearDownTest() {
    // Per-test cleanup
}

// Run parallel test groups
func TestIntegrationSmallObjects(t *testing.T) { suite.Run(t, new(SmallObjectSuite)) }
func TestIntegrationMediumObjects(t *testing.T) { suite.Run(t, new(MediumObjectSuite)) }
func TestIntegrationLargeObjects(t *testing.T) { suite.Run(t, new(LargeObjectSuite)) }
func TestIntegrationCompaction(t *testing.T) { suite.Run(t, new(CompactionSuite)) }
func TestIntegrationWorkflows(t *testing.T) { suite.Run(t, new(WorkflowSuite)) }
```

## Success Criteria

### Functional Requirements
- ✅ All object sizes stored in correct location (inline/raw/segment)
- ✅ Compaction moves only eligible medium files (< 16MB) to segments
- ✅ Large files (> 16MB) remain as permanent raw files
- ✅ No data loss during concurrent operations
- ✅ TTL and LRU cleanup work as specified
- ✅ Recovery from failures maintains data integrity

### Performance Requirements
- ✅ Small object operations < 1ms p99 latency
- ✅ Large object streaming > 100MB/s throughput
- ✅ Compaction completes within 5 minutes for 10GB data
- ✅ Memory usage < 1GB for 100GB stored data
- ✅ FD usage < 1000 for any workload

### Reliability Requirements
- ✅ Zero data corruption across all tests
- ✅ Graceful degradation under resource limits
- ✅ Clean shutdown and startup procedures
- ✅ No goroutine or memory leaks
- ✅ Consistent behavior across repeated runs

## Test Environment Configuration

### Resource Limits
```yaml
test_environment:
  memory_limit: 2GB
  disk_space: 500GB
  file_descriptors: 1024
  cpu_cores: 4
```

### Test Data Volumes
```yaml
data_volumes:
  small_objects: 10000 objects (≈100MB)
  medium_objects: 1000 objects (≈5GB)
  large_objects: 100 objects (≈10GB)
  total_dataset: ≈15GB
```

### Timing Configuration
```yaml
timing:
  compaction_interval: 1s (test) / 5m (production)
  cleanup_interval: 1s (test) / 1m (production)
  test_timeout: 10m per test / 2h for full suite
  stabilization_wait: 100ms after operations
```

## Monitoring and Reporting

### Metrics to Collect
- Operation latencies (read/write/delete)
- Storage distribution (inline/raw/segment counts)
- Compaction efficiency (files processed/minute)
- Resource usage (memory/disk/CPU/FDs)
- Error rates and types
- Background process execution times

### Test Reports
- Per-test pass/fail status
- Performance regression detection
- Resource usage trends
- Coverage metrics
- Failure analysis and logs

## Execution Plan

### Phase 1: Foundation (Week 1)
1. Set up test infrastructure and utilities
2. Implement data generators and verifiers
3. Create test harness and configuration

### Phase 2: Component Tests (Week 2-3)
1. Implement small object test suite
2. Implement medium object test suite
3. Implement large object test suite
4. Add compaction-specific tests

### Phase 3: Integration Tests (Week 4)
1. Implement cross-component workflows
2. Add background process coordination tests
3. Create stress and performance tests

### Phase 4: Validation (Week 5)
1. Run full test suite repeatedly
2. Fix any flaky tests
3. Optimize test execution time
4. Document results and findings

## Maintenance

### Regular Updates
- Add tests for new features
- Update thresholds if defaults change
- Adjust performance baselines
- Enhance failure scenarios

### CI/CD Integration
- Run subset on every commit
- Full suite on nightly builds
- Performance tests weekly
- Stress tests before releases