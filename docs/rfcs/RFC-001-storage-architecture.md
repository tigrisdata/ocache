# RFC-001: Storage Architecture Overview

**RFC Number:** 001  
**Status:** Active  
**Authors:** Ovais Tariq
**Created:** 2025-06-05  
**Last Updated:** 2025-09-04

## Abstract

OCache implements a multi-tiered storage architecture that intelligently routes objects based on size to optimize for both performance and storage efficiency. The system uses RocksDB for small objects (metadata and inline storage), raw files for medium-to-large objects, and a segment-based storage system for compacted medium objects. This RFC describes the overall storage architecture, the rationale behind key design decisions, and how different components interact to provide a high-performance caching solution.

## Motivation

Modern caching workloads present diverse requirements:

- **Small objects** (< 64KB): Highly concurrent access where latency is critical, benefit from in-memory caching and fast key-value lookups
- **Medium objects** (64KB - 16MB): Moderate concurrency access patterns, benefit from consolidation to reduce file handle usage
- **Large objects** (> 16MB): Low concurrency with high throughput requirements, streaming capabilities, benefit from direct file access

Traditional single-tier storage systems force trade-offs that compromise performance for certain object sizes. OCache's architecture addresses these challenges by:

1. Optimizing I/O patterns for different object sizes
2. Providing predictable performance across the entire size spectrum
3. Supporting efficient TTL and LRU eviction at scale

## Design Overview

### Three-Tier Storage Model

#### Storage Tier Selection Flow

This flowchart shows how OCache determines the appropriate storage tier for incoming objects based on their size:

```mermaid
flowchart TD
    Start([Client Write Request]) --> GetSize[Calculate Object Size]
    
    GetSize --> CheckSmall{Size < 64KB?}
    
    CheckSmall -->|Yes| InlineStorage[Store in RocksDB<br/>as INLINE type]
    CheckSmall -->|No| CheckMedium{Size < 16MB?}
    
    CheckMedium -->|Yes| RawFile[Write to Raw File<br/>Mark as COMPACTABLE]
    CheckMedium -->|No| LargeFile[Write to Raw File<br/>Mark as PERMANENT]
    
    InlineStorage --> WriteMeta1[Write Metadata<br/>to RocksDB]
    RawFile --> WriteMeta2[Write Metadata<br/>to RocksDB]
    LargeFile --> WriteMeta3[Write Metadata<br/>to RocksDB]
    
    WriteMeta1 --> UpdateAccess1[Update Access Index]
    WriteMeta2 --> UpdateAccess2[Update Access Index]
    WriteMeta3 --> UpdateAccess3[Update Access Index]
    
    UpdateAccess1 --> Success([Return Success])
    UpdateAccess2 --> Success
    UpdateAccess3 --> Success

    style Start fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    style InlineStorage fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    style RawFile fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
    style LargeFile fill:#795548,stroke:#4E342E,stroke-width:2px,color:#fff
    style Success fill:#9C27B0,stroke:#6A1B9A,stroke-width:2px,color:#fff
```

#### Client Read Request Flow

This flowchart shows how OCache retrieves objects from different storage tiers based on metadata:

```mermaid
flowchart TD
    Start([Client Read Request]) --> GetMeta[Lookup Key in RocksDB<br/>Retrieve Metadata]
    
    GetMeta --> CheckMeta{Metadata<br/>Found?}
    
    CheckMeta -->|No| NotFound[Return Not Found]
    CheckMeta -->|Yes| CheckTTL{TTL<br/>Expired?}
    
    CheckTTL -->|Yes| Expired[Mark for Cleanup<br/>Return Not Found]
    CheckTTL -->|No| CheckType{Check<br/>ValueType}
    
    CheckType -->|INLINE| ReadInline[Return Data<br/>from Metadata]
    CheckType -->|RAW_FILE| ReadRaw[Open Raw File<br/>via FD Cache]
    CheckType -->|SEGMENT| ReadSegment[Open Segment File<br/>via FD Cache]
    
    ReadRaw --> CheckRawFile{File<br/>Exists?}
    CheckRawFile -->|No| Retry1[Retry with<br/>Exponential Backoff]
    CheckRawFile -->|Yes| StreamRaw[Create File Reader<br/>Stream Data]
    
    ReadSegment --> SeekOffset[Seek to Offset<br/>in Segment]
    SeekOffset --> CheckSegFile{Segment<br/>Valid?}
    CheckSegFile -->|No| Retry2[Retry with<br/>Exponential Backoff]
    CheckSegFile -->|Yes| StreamSeg[Create Bounded Reader<br/>Stream Data]
    
    ReadInline --> UpdateLRU[Queue Async<br/>Access Update]
    StreamRaw --> UpdateLRU
    StreamSeg --> UpdateLRU
    
    UpdateLRU --> ReturnData([Return Data<br/>to Client])
    
    Retry1 --> CheckRetry1{Max<br/>Retries?}
    Retry2 --> CheckRetry2{Max<br/>Retries?}
    
    CheckRetry1 -->|No| ReadRaw
    CheckRetry1 -->|Yes| Error1[Return Error]
    
    CheckRetry2 -->|No| ReadSegment
    CheckRetry2 -->|Yes| Error2[Return Error]

    style Start fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    style ReadInline fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    style StreamRaw fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
    style StreamSeg fill:#FF5722,stroke:#BF360C,stroke-width:2px,color:#fff
    style ReturnData fill:#9C27B0,stroke:#6A1B9A,stroke-width:2px,color:#fff
    style NotFound fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
    style Expired fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
    style Error1 fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
    style Error2 fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
```

#### Compaction and Recompaction Process

This diagram illustrates the background compaction and recompaction lifecycle for medium-sized objects:

```mermaid
flowchart LR
    subgraph "Initial Storage"
        RF1[Raw File 1<br/>100KB] 
        RF2[Raw File 2<br/>200KB]
        RF3[Raw File 3<br/>150KB]
        RF4[Raw File 4<br/>500KB]
    end
    
    subgraph "Compaction Process"
        Scanner[Compactor Scanner<br/>Finds eligible files<br/>64KB - 16MB]
        Batch[Batch Creator<br/>Groups files<br/>Target: 256MB]
        Write[Segment Writer<br/>Creates new segment]
    end
    
    subgraph "Segment Storage"
        Seg1[Segment 1<br/>256MB<br/>Contains RF1-3]
        Seg2[Segment 2<br/>256MB<br/>Contains RF4+others]
    end
    
    subgraph "Recompaction Process"
        Monitor[Fragment Monitor<br/>Checks dead space]
        Check{Fragmentation<br/>> 50%?}
        Age{Segment Age<br/>> 2 hours?}
        Recompact[Recompactor<br/>Copies live data]
    end
    
    subgraph "Recompacted Storage"
        NewSeg[New Segment<br/>Live data only]
    end
    
    RF1 --> Scanner
    RF2 --> Scanner
    RF3 --> Scanner
    RF4 --> Scanner
    
    Scanner --> Batch
    Batch --> Write
    Write --> Seg1
    Write --> Seg2
    
    Seg1 --> Monitor
    Seg2 --> Monitor
    
    Monitor --> Check
    Check -->|Yes| Age
    Check -->|No| Monitor
    
    Age -->|Yes| Recompact
    Age -->|No| Monitor
    
    Recompact --> NewSeg
    
    Cleanup[Cleanup Process<br/>Deletes old files<br/>after grace period]
    
    Seg1 -.->|After recompaction| Cleanup
    RF1 -.->|After compaction| Cleanup
    RF2 -.->|After compaction| Cleanup
    RF3 -.->|After compaction| Cleanup
    RF4 -.->|After compaction| Cleanup

    style RF1 fill:#607D8B,stroke:#37474F,stroke-width:2px,color:#fff
    style RF2 fill:#607D8B,stroke:#37474F,stroke-width:2px,color:#fff
    style RF3 fill:#607D8B,stroke:#37474F,stroke-width:2px,color:#fff
    style RF4 fill:#607D8B,stroke:#37474F,stroke-width:2px,color:#fff
    style Scanner fill:#3F51B5,stroke:#283593,stroke-width:2px,color:#fff
    style Batch fill:#009688,stroke:#00695C,stroke-width:2px,color:#fff
    style Write fill:#00BCD4,stroke:#00838F,stroke-width:2px,color:#fff
    style Seg1 fill:#FF5722,stroke:#BF360C,stroke-width:2px,color:#fff
    style Seg2 fill:#FF5722,stroke:#BF360C,stroke-width:2px,color:#fff
    style Monitor fill:#673AB7,stroke:#4527A0,stroke-width:2px,color:#fff
    style Recompact fill:#E91E63,stroke:#AD1457,stroke-width:2px,color:#fff
    style NewSeg fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    style Cleanup fill:#9E9E9E,stroke:#424242,stroke-width:2px,color:#fff
```

### Key Components

1. **RocksDB Layer**: Stores all metadata and small objects inline
2. **File Manager**: Manages raw file creation, reading, and deletion
3. **Segment Manager**: Manages compacted segments with multiple objects
4. **Compactor**: Background process that migrates raw files to segments
5. **Cleaner**: Background process for TTL expiration and LRU eviction
6. **Access Updater**: Tracks object access patterns for LRU decisions

## Detailed Design

### Storage Routing Logic

The storage layer makes routing decisions based on object size:

```
FUNCTION routeObject(key, value):
    size = length(value)

    IF size < InlineThreshold:          // Default: 64KB
        // Small objects: optimize for high concurrency and low latency
        RETURN INLINE                    // Store in RocksDB

    ELSE IF size < CompactThreshold:    // Default: 16MB
        // Medium objects: balance between performance and resource usage
        RETURN RAW_FILE                  // Store as raw file, eligible for compaction

    ELSE:
        // Large objects: optimize for throughput with streaming
        RETURN RAW_FILE_PERMANENT        // Store as raw file, never compact
    END IF
END FUNCTION
```

### Metadata Structure

Every object, regardless of storage location, has metadata stored in RocksDB:

```protobuf
message ValueMessage {
  ValueType value_type = 1;    // INLINE, RAW_FILE, or SEGMENT
  bytes data = 2;               // Inline data (if applicable)
  int64 expiry = 3;             // TTL expiry timestamp
  string raw_file_path = 4;     // Path to raw file
  string segment_path = 5;      // Path to segment file
  int64 segment_offset = 6;     // Offset within segment
  int64 value_length = 7;       // Length of value
  uint32 checksum = 8;          // CRC32 checksum
}
```

### Write Path

1. **Size Classification**: Determine storage tier based on object size
2. **Metadata Creation**: Create ValueMessage with appropriate storage location
3. **Data Storage**:
   - **Inline**: Store data directly in RocksDB with metadata
   - **Raw File**: Write to new file, store path in metadata
   - **Segment**: Initially write to raw file (compaction handles migration)
4. **Atomic Commit**: Write metadata to RocksDB (ensures consistency)
5. **Access Tracking**: Update access index for LRU tracking

### Read Path

1. **Metadata Lookup**: Retrieve ValueMessage from RocksDB
2. **Data Retrieval** based on ValueType:
   - **Inline**: Return data from ValueMessage
   - **Raw File**: Open file using FD cache, return reader
   - **Segment**: Open segment, seek to offset, return bounded reader
3. **Access Update**: Queue asynchronous access time update
4. **Error Handling**: Retry with exponential backoff for transient errors

### Background Processes

#### Compactor

- Scans for raw files eligible for compaction (64KB - 16MB)
- Batches multiple files into segments (target: 256MB segments)
- Updates metadata atomically after successful compaction
- Deletes original raw files after grace period

#### Recompactor

- Monitors segment fragmentation (deleted space ratio)
- Triggers when fragmentation exceeds threshold (default: 50%)
- Copies live data to new segments
- Ensures segments are "cold" before recompaction (default: 2 hours old)

#### Cleaner

- Periodically scans for expired TTL entries
- Implements LRU eviction when disk usage exceeds threshold
- Cleans up orphaned files from failed operations
- Maintains access index buckets (24-hour granularity)

## Trade-offs and Alternatives

### Considered Alternatives

1. **Single-Tier File Storage**

   - Pros: Simpler implementation, uniform handling
   - Cons: Excessive file descriptors, poor small object performance
   - Decision: Rejected due to scalability limitations

2. **Pure LSM-Tree (RocksDB for everything)**

   - Pros: Proven technology, good write amplification
   - Cons: Large objects cause write amplification, compaction overhead
   - Decision: Rejected for large object inefficiency

3. **Fixed-Size Blocks**
   - Pros: Predictable I/O patterns, simple space management
   - Cons: Internal fragmentation, complex for variable sizes
   - Decision: Rejected for space inefficiency

### Design Trade-offs

1. **Inline Threshold (64KB)**

   - Lower: Less RocksDB memory usage, more files
   - Higher: Better small object performance, more memory usage
   - Choice: 64KB balances memory and file descriptor usage

2. **Compaction Threshold (16MB)**

   - Lower: More objects compacted, better FD usage
   - Higher: Less compaction overhead, simpler large objects
   - Choice: 16MB prevents excessive segment churn

3. **Segment Size (256MB)**
   - Smaller: Faster compaction, more segments
   - Larger: Better sequential I/O, fewer segments
   - Choice: 256MB optimizes for modern SSD performance

## Performance Considerations

### Optimizations

1. **Read-Only Segments**: Immutable segments enable lock-free reads
2. **Buffer Pooling**: Reusable buffers reduce GC pressure
3. **Batch Operations**: Compaction and cleanup work in batches
4. **Async Access Updates**: Access tracking doesn't block reads
5. **File Descriptor Caching**: LRU cache with 10,000 entries prevents syscall overhead

## References

- [RocksDB Architecture](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)
- [Log-Structured Merge Trees](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
