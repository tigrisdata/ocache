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
