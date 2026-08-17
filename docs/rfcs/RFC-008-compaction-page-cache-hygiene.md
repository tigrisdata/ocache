# RFC-008: Compaction Page Cache Hygiene

**RFC Number:** 008  
**Status:** Active  
**Authors:** Ovais Tariq  
**Created:** 2026-08-17  
**Last Updated:** 2026-08-17

## Abstract

This RFC describes how the compaction system keeps its bulk I/O out of the operating system page cache. Compaction is a one-pass background workload: it reads each raw file or fragmented segment exactly once and writes each output segment sequentially, never rereading what it wrote. Left unhinted, the kernel caches all of that data, evicting the warm serving working set in the process. The design records the output ranges written during a compaction batch and discards them with `posix_fadvise(FADV_DONTNEED)` strictly after the containing segment has been synced, and discards one-pass input pages immediately after they are copied. The hint is best-effort by construction: correctness and crash safety never depend on it, and non-Linux platforms compile to a no-op.

## Motivation

In production, cache nodes run with the page cache as the primary warm-read accelerator: objects served from segments rely on their pages staying resident between requests. The compaction system competes with that working set for the same cache:

1. **Cache pollution from outputs**: Every byte the compactor writes into a segment lands in the page cache, even though the compactor never reads it back. A compaction batch that consolidates hundreds of MiB displaces an equal amount of warm serving data.
2. **Cache pollution from inputs**: Raw files and old segments are read exactly once during consolidation, yet their pages remain cached after the copy — pure waste, since the source is deleted shortly after.
3. **Measured impact**: On throughput-capped volumes (240 MB/s), compaction bursts reaching ~2 GB/s of cluster-wide buffered I/O coincided with multi-second serving-read stalls, and warm serves were observed re-reading 137–358 MiB/s from disk because their pages had been evicted by populate and compaction churn.

The compaction data is the *coldest* data on the node — it was selected for consolidation precisely because it is not hot — so it is the last thing the page cache should retain at the expense of actively served objects.

## Design Overview

### The `cacheAdvice` tracker

Each `CompactFiles` batch creates one `cacheAdvice` value that accumulates the output ranges written during the batch:

```go
type cacheRange struct {
    offset int64
    length int64
}

type cacheAdvice struct {
    ranges map[*segment.Segment]cacheRange
    drop   func(path string, offset, length int64)
}
```

- `addOutput(seg, offset, length)` records the range just written to `seg`, merging it with any previously recorded range for the same segment into a single covering range. Ranges only grow; nothing is dropped at record time.
- `dropSyncedOutput(seg)` removes the accumulated range for `seg` and issues the drop callback for it. It is idempotent — a second call for the same segment is a no-op.

The indirection through the `drop` function keeps the tracker unit-testable without touching the kernel.

### Drop points

There are exactly three places pages are discarded, chosen so that a page is only ever dropped when it is (a) one-pass input already consumed, or (b) output that has been durably synced:

1. **Input, after copy** (`copyFileIntoSegment`, and the old-segment read path in the recompactor): once the raw file or old-segment range has been copied into the new segment, its cache pages serve no future read. They are dropped immediately. Dirty input pages cannot exist at this point for raw files (the file was written and closed long before compaction selected it); if any were dirty, `FADV_DONTNEED` silently skips them.
2. **Output, at segment rollover** (`ensureCapacity`): when the current segment fills up, it is finalized — which syncs it — and then its accumulated output range is dropped before a fresh segment is acquired.
3. **Output, at batch commit** (`commit`): the final segment of the batch is synced (`seg.Sync()`) before the RocksDB metadata batch is written; the accumulated range is dropped immediately after that sync.

### Sync-before-drop invariant

`FADV_DONTNEED` on a dirty page is at best ignored and at worst triggers writeback at an uncontrolled moment. More importantly, the compaction system's crash-safety contract is *segment durable before metadata visible*. The advice mechanism is therefore subordinated to that ordering:

```
write entries → addOutput(range) → seg.Sync() / FinalizeSegment() → dropSyncedOutput(seg) → metadata publish
```

A drop can never precede the sync of the range it covers, because `dropSyncedOutput` is only called at the two points in the code that immediately follow a successful `Sync`/`Finalize`. If the process crashes between sync and drop, the only consequence is that some pages stay cached — the hint is advisory, so no recovery logic exists or is needed.

As part of establishing this invariant, the recompactor gained an explicit `newSeg.Sync()` before it publishes metadata pointing at the new segment. Previously the recompacted segment's durability relied on later incidental syncs; the ordering is now explicit and matches Phase 1.

### Platform behavior

- **Linux** (`cache_advice_linux.go`): `unix.Fadvise(fd, offset, length, FADV_DONTNEED)`, return value deliberately ignored.
- **Everything else** (`cache_advice_other.go`): `dropFileCache` is an empty function. macOS development builds and tests exercise the full tracker logic with the no-op sink.

### What this RFC deliberately does not do

- **No `O_DIRECT`**: direct I/O would bypass the cache entirely but imposes alignment constraints on every read and write path, changes error semantics, and forfeits kernel write coalescing. The advisory approach achieves the same cache outcome for synced data with zero change to the I/O paths.
- **No `sync_file_range` pacing**: writeback scheduling is left to the kernel. Rate-limiting compaction throughput is a separate concern addressed independently (shared byte-budget across compaction workers).
- **No dropping of serving segments' warm ranges**: only ranges the compactor itself wrote or consumed in this batch are ever dropped. A segment that receives serving reads keeps whatever residency those reads create.

## Validation

Measured with `mincore(2)`-based residency accounting in memory-limited cgroups (benchmarks in `storage/compaction_serving_bench_linux_test.go`; a warmed 64 MiB serving segment is read continuously while compaction runs at production thresholds):

| Metric | Before | After |
| --- | --- | --- |
| File compaction: resident compaction pages per batch | 100,642 (~393 MiB), every run | 1–48k (mean ~40k) |
| Recompaction: resident output pages per batch | 32,513 (~127 MiB), every run | **1 page**, every run |
| Serving-read p95 while recompaction runs | 77–90 µs | 11–20 µs (4–7×) |

Recompaction output residency is fully eliminated because the drop directly follows `Sync()`, when all pages are clean. File compaction retains a fraction of input pages whose writeback had not completed when the hint fired — the expected best-effort behavior; retention shrinks as writeback catches up.

## Failure Modes

| Failure | Consequence |
| --- | --- |
| `fadvise` unsupported (filesystem, kernel, container runtime) | Pages stay cached; behavior identical to before this RFC |
| Range still dirty at drop time | Kernel skips those pages; they age out normally |
| Crash between sync and drop | Pages stay cached until reclaim; no correctness impact |
| Drop of a range another reader is using | Cannot occur for outputs (metadata not yet published, so no reader can reference the range); inputs are dropped only after the copy completes and are deleted shortly after |

## References

- [RFC-003: Compaction System Design](RFC-003-compaction-system.md) — the two-phase compaction architecture this RFC extends
- [RFC-002: Segment Storage](RFC-002-segment-storage.md) — segment write/sync/finalize lifecycle
- `posix_fadvise(2)`, `mincore(2)`
