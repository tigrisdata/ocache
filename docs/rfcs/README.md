# OCache RFCs

Design documents for OCache's architecture and major features.

## Active RFCs

| RFC                                        | Title                | Description                                                                           |
| ------------------------------------------ | -------------------- | ------------------------------------------------------------------------------------- |
| [001](RFC-001-storage-architecture.md)     | Storage Architecture | Multi-tiered storage: RocksDB (small), raw files (medium/large), segments (compacted) |
| [002](RFC-002-segment-storage.md)          | Segment Storage      | Packing medium objects (64KB-16MB) into consolidated segment files                    |
| [003](RFC-003-compaction-system.md)        | Compaction System    | Background service migrating raw files to segments                                    |
| [004](RFC-004-file-management-recovery.md) | File Management      | Raw file lifecycle, crash recovery, orphan detection                                  |
| [005](RFC-005-ttl-eviction-system.md)      | TTL & Eviction       | Time-based expiration and LRU eviction strategies                                     |
| [006](RFC-006-rocksdb-integration.md)      | RocksDB Integration  | Metadata store and small object storage configuration                                 |
| [007](RFC-007-sharded-cluster.md)          | Sharded Cluster      | Distributed sharding with consistent hashing and high availability                    |
| [008](RFC-008-compaction-page-cache-hygiene.md) | Compaction Page Cache Hygiene | Dropping compaction I/O from the page cache with fadvise to protect warm serving reads |
| [009](RFC-009-walk-gated-recompaction.md) | Walk-Gated Recompaction | Recompaction gated by ground-truth liveness derivation per segment; the delete index demotes to a prioritization hint |

Start with [RFC-001](RFC-001-storage-architecture.md) for an architecture overview.
