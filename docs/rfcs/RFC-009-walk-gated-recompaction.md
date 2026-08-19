# RFC-009: Walk-Gated Recompaction

**RFC Number:** 009  
**Status:** Active  
**Authors:** Ovais Tariq  
**Created:** 2026-08-18  
**Last Updated:** 2026-08-19

## Abstract

This RFC changes what triggers segment recompaction. Today the trigger is the per-segment delete index — an incremental counter that is correct only if every mutation path credits it exactly once, forever. History shows that invariant breaks (#218, #224, #225), and every missed credit is permanent: dead bytes have no metadata pointing at them, so no other reclaim path can ever reach them, and the recompactor — the only mechanism that frees them — is gated on the very counter that failed. Instead of trusting the counter, the recompaction loop now derives each candidate segment's dead bytes from ground truth — walking the segment's own entry headers and point-looking-up each key's metadata row, the same liveness test `recompactSegment` already applies before copying — and lets that derivation gate the rewrite directly. The delete index demotes to a prioritization hint. A wrong hint now costs at most one wasted walk (single-digit milliseconds) instead of leaked or pointlessly rewritten data.

## Motivation

Dead segment bytes are reclaimable only through recompaction, and recompaction previously triggered only when the delete index reported fragmentation above threshold. Under-credit therefore deadlocked reclaim: the reset mechanism fired only when the counter — which read too low — said so. Production impact of exactly this failure: fleets grew physical disk 20–25 GiB/h against a healthy logical cap, with `ocache_recompaction_segments_total` pinned at zero, ending in ENOSPC and full cache wipes.

The incremental-credit fixes (#218, #223, #225, #226) close the known systematic sources, but the architecture still assumed incremental perfection: any future missed or doubled credit — a crash window, a racing overwrite, the next bug — silently re-opens the leak. Two designs for repairing the counter were prototyped and rejected: a scan-based reconciliation (hourly or at startup) requires a full metadata pass and is unsafe concurrent with the compactor's minutes-long finalize→commit publication window; a separate audit correcting the index still left two mechanisms where one suffices.

The unifying observation: **deriving a segment's liveness is cheap enough to be the gate itself.** Only objects above the inline threshold (64 KiB) reach segments, so a 256 MiB segment holds at most ~4,096 entries — typically ~256. A derivation is a header-hop through the segment file (small sequential reads, no payloads) plus that many RocksDB point lookups: single-digit milliseconds.

## Design

### The walk

`walkSegmentLiveness` derives a closed segment's dead entries and bytes from ground truth:

```
dead = footer totals − Σ entries whose metadata row still points at this segment+offset
```

The footer supplies totals (`numEntries`, `dataBytes`; restored across restarts by #226). Liveness comes from walking the segment's entry headers — each header carries its key — and point-getting each key's metadata row; an entry is live iff the row exists, is `SEGMENT`-typed, and points at this segment and offset. This is byte-for-byte the test `recompactSegment` performs before copying an entry.

Failure handling is strict, because deriving from partial knowledge would count unseen live entries dead:

- Only `ErrMetadataNotFound` means dead; a transient lookup failure aborts the walk.
- The segment iterator reads file truncation as a clean early EOF, so the walk cross-checks its entry count against the footer and aborts on mismatch — the footer is what distinguishes damage from end-of-entries.
- An aborted walk leaves the segment untouched; it is retried next interval.

### Selection and gating

Each recompaction pass (default 1/min):

1. **Candidates**: every closed segment past the existing `MinSegmentAgeForRecompaction` gate, excluding segments derived within the walk interval (default 1 h, in-memory recency — a restart just restarts the rotation). Hint growth since the last walk bypasses the interval.
2. **Order**: delete-index hint bytes descending, then walk staleness — reclaim-worthy segments are walked and recompacted first.
3. **Gate**: walk each candidate; if derived `deadBytes / size ≥ FragThreshold`, recompact it in the same pass via the existing copy path (whose per-entry re-verification also handles entries that die between walk and copy).

There is deliberately **no per-pass count cap**: walk reads draw from the shared compaction I/O limiter, so even a whole-fleet pass — the first after a restart, or the hourly re-expiry — is paced (a 3,000-segment fleet at typical entry sizes is ~90 s of budgeted I/O) rather than bursty, and the pass checks cancellation per candidate.

The delete index is consulted only for ordering. Its credits remain worth keeping accurate — they are what make prioritization responsive — so `DeleteKey` and the TTL cleaner now merge segment credit **in the same batch** as the metadata delete (the discipline the Put-overwrite fix introduced in #225/#228), eliminating the crash windows between commit and credit in either direction. But no correctness depends on them anymore: the historical orphan backlog, credits lost to any future bug, and inflated credits from racing writers are all absorbed by the walk, which converges within one rotation.

### Safety

- **Publication races**: the danger is walking a segment whose entries' metadata rows still sit in an in-flight compaction run's uncommitted batch — they would derive as dead. The age gate excludes this: compaction runs are byte-bounded, so their finalize→commit window is minutes even at the I/O-budget floor, far under the 2 h gate. This is the same argument recompaction has always relied on for the same hazard.
- **Concurrent mutation**: closed segments accept no new entries, so liveness only decreases during a walk. An entry deleted mid-walk counts live for this pass — conservative; the next rotation corrects.
- **Crash recovery**: a segment finalized by a run whose metadata commit never happened contains only dead copies (their rows still reference the surviving raw files). The walk derives them dead and the segment is reclaimed — previously these bytes were permanently invisible.

### Cost

Each entry visit touches ~one 4 KiB page of the segment file (header and key share it): ~1 MiB of physical read per 256 MiB segment at typical 1 MiB entries, 16 MiB at the 64 KiB worst case. Steady-state walk volume is fleet-size / walk-interval (~50 segments/min for 3,000 segments — ~0.8 MiB/s typical); to keep the small-read burst from landing as an IOPS spike, **walk reads draw from the same shared compaction I/O limiter as payload copies** (one page-cost reservation per entry), so walks, compaction, and recompaction together stay under the configured `CompactionBytesPerSecond` ceiling. Metadata point lookups are not charged (block-cache hits, no meaningful byte cost). No metadata scan is ever taken; memory is O(live segments) for walk recency.

### Responsiveness

Walk recency records the delete-index hint observed at derivation time. Hint **growth** since the last walk bypasses the re-walk interval, so credited deletions are re-derived on the very next pass — matching the old gate's reactivity — while the interval only bounds re-walks for segments whose hint is unchanged (the drift the credits missed). A derived-fragmented segment whose rewrite fails is retried on the next pass, not parked for the interval: reclaim matters most under the disk pressure that makes rewrites fail.

## Metrics

| Metric | Meaning |
| --- | --- |
| `ocache_segment_walks_total` | Segments walked (liveness derived) |
| `ocache_recompaction_segments_total` | Segments recompacted (existing) — now driven by derived truth |

## Failure Modes

| Failure | Consequence |
| --- | --- |
| Segment read error / truncation mid-walk | Walk aborts; segment untouched; retried next interval |
| Metadata point-lookup transient error | Walk aborts (never counted dead); retried next interval |
| Hint read failure | Segment loses priority only; rotation still covers it |
| Stale/inflated hint | At most one wasted ~ms walk per interval |

## Future work

Every entry header already stores a CRC32 of its payload that nothing verifies after write. The walk is the natural host for an integrity scrub tier — read payloads on every Nth rotation, verify, and invalidate corrupted entries for upstream refetch (the cache's upstream is authoritative, so eviction is the safe repair). Deliberately deferred to keep this change single-purpose.

## Relationship to prior work

- #218/#223/#224/#225/#226: close the known systematic credit-loss sources; this RFC removes the *dependence* on their completeness.
- [RFC-003](RFC-003-compaction-system.md): the recompaction phase this RFC re-gates.
- Supersedes the scan-based reconciliation prototyped and withdrawn in #228, and the audit-corrects-index design prototyped and withdrawn on this PR's earlier revisions.
