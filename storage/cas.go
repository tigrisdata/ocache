// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/compaction"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	pb "github.com/tigrisdata/ocache/storage/proto"
)

// Conditional (CAS) operations — issue #254.
//
// A CAS is a merge operand on the key's metadata row, resolved deterministically
// by mergeMetadataCAS against whatever base precedes it in RocksDB's per-key
// sequence order. That ordering is the serialization point: there is no
// read-check-write window and therefore no lock — on this path or any other.
// The outcome is learned by reading the row back (read-your-writes): the winner
// sees its own stamp, a loser sees whoever beat it.
//
// Absence has history (issue #267). A CAS delete leaves a ref-less tombstone
// stamped with the delete's version, and that stamp is the key's FENCE until
// the TTL sweep ages it out (fence retention). An absent read hands out an
// observation token — the fence stamp, or a fresh stamp for a key with no
// fence — and a put carrying a token from BEFORE a fence loses to it, so a
// populate that fetched stale bytes cannot land over an invalidation. Passing
// expected == 0 opts out of ordering (put-if-absent). Delete-if-absent on a
// missing or dead key records or moves the fence. TTL expiry is unfenced: an
// expired row reads absent with a fresh token, and recreating over it is
// best-effort with the same cleaner-race semantics as a plain Put. Live legacy
// (pre-versioning) rows match merge.VersionLegacy. A CAS_DELETE reclaims the
// replaced value's backing bytes on its confirmed win, since the ref-less
// tombstone it leaves carries no references for the cleaner to act on. Plain
// Put/Get/Delete are untouched by all of this; mixing plain and CAS ops on one
// key voids the guarantees, as it always has for versions.

// versionReservationBlock is how far past the current stamp each durable
// reservation extends — one minute of nanosecond stamps. Reservations are
// persisted (synced) BEFORE any stamp above the previous reservation is
// issued, so after a crash the restored high-water mark is >= every stamp
// ever handed out, keeping versions monotonic across restarts even when the
// wall clock steps backward (a repeated stamp would let a retained token
// match a different row generation). Amortized cost: one synced point write
// per minute of issued-stamp range; the per-stamp cost is one atomic load.
const versionReservationBlock = uint64(60 * 1e9)

// loadVersionReservation restores the durable stamp ceiling at startup and
// seats lastVersion at it, so the first stamp issued this run is strictly
// above anything issued before the restart.
func (s *Storage) loadVersionReservation() error {
	slice, err := s.meta.Handle().Get(putPointReadOpts, []byte(keys.VersionHWMKey))
	if err != nil {
		return err
	}
	defer slice.Free()
	if slice.Exists() && len(slice.Data()) == 8 {
		hwm := binary.BigEndian.Uint64(slice.Data())
		s.lastVersion.Store(hwm)
		s.versionHi.Store(hwm)
	}
	return nil
}

// extendVersionReservation durably raises the stamp ceiling to cover next.
// Called off the fast path (roughly once per versionReservationBlock of stamp
// range); the write is synced so a crash cannot forget a reservation that
// stamps were issued under.
func (s *Storage) extendVersionReservation(next uint64) error {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	if next <= s.versionHi.Load() { // another caller extended while we waited
		return nil
	}
	newHi := next + versionReservationBlock
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newHi)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wo.SetSync(true)
	if err := s.meta.Handle().Put(wo, []byte(keys.VersionHWMKey), buf); err != nil {
		return err
	}
	s.versionHi.Store(newHi)
	return nil
}

// nextVersion issues the node-local monotonic version stamp: the wall clock in
// nanoseconds, bumped past the previously issued stamp so concurrent calls and
// clock steps can never repeat or regress a version on this node — and never
// above the durably reserved ceiling without extending it first, so stamps
// cannot repeat across restarts either. Stamps are nanosecond-scale, so they
// can never collide with 0 ("absent") or merge.VersionLegacy.
func (s *Storage) nextVersion() (uint64, error) {
	for {
		now := uint64(time.Now().UnixNano())
		last := s.lastVersion.Load()
		next := now
		if next <= last {
			next = last + 1
		}
		if next > s.versionHi.Load() {
			if err := s.extendVersionReservation(next); err != nil {
				return 0, err
			}
		}
		if s.lastVersion.CompareAndSwap(last, next) {
			return next, nil
		}
	}
}

// casStatus classifies a CAS operation's final outcome into the metrics `status`
// label. Centralizing the success/mismatch/error decision here (called once from
// each op's deferred recorder) keeps the classification out of the per-branch
// worker code.
func casStatus(err error) string {
	if err == nil {
		return "success"
	}
	if _, ok := storageErrors.IsVersionMismatch(err); ok {
		return "mismatch"
	}
	return "error"
}

// readRowForCAS reads and decodes the key's full metadata row. found reports
// whether the row exists at all (live or expired).
func (s *Storage) readRowForCAS(metaKey []byte) (*pb.ValueMessage, bool, error) {
	slice, err := s.meta.Handle().Get(putPointReadOpts, metaKey)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil, false, nil
	}
	vm := &pb.ValueMessage{}
	if err := proto.Unmarshal(slice.Data(), vm); err != nil {
		return nil, false, err
	}
	return vm, true, nil
}

// readRowForCASRetry re-reads a row a few times before giving up. It is used for
// the post-merge read-back of both CAS writes, where a transient read glitch
// (the merge already committed) would otherwise force an indeterminate outcome:
// for a put it would leak a losing spill, and for a delete it would skip the
// immediate reclaim of the replaced value's backing bytes on a win. Resolving
// the outcome lets the caller reclaim correctly; only a persistent read failure
// (a genuinely unhealthy DB) is surfaced.
func (s *Storage) readRowForCASRetry(metaKey []byte) (vm *pb.ValueMessage, found bool, err error) {
	const attempts = 4
	for i := 0; i < attempts; i++ {
		vm, found, err = s.readRowForCAS(metaKey)
		if err == nil {
			return vm, found, nil
		}
	}
	return nil, false, err
}

// GetWithVersion returns the value reader together with the key's current CAS
// version (issue #254). It is a CAS-path operation that does not touch the plain
// Get read path. The value reader and the version come from a SINGLE metadata
// read, so the pair is one atomic snapshot (a second read via Get could pair one
// generation's data with another's version).
//
// An absent key reports found == false together with an OBSERVATION TOKEN
// (issue #267), never 0: the stamp of the CAS delete that removed it while that
// fence is retained, otherwise a fresh stamp meaning "absent as of now". Pass
// the token back as expected to PutIfVersion to order the write against any
// delete: a delete stamped after the observation rejects it. Pass 0 instead for
// the unordered put-if-absent. A live pre-versioning (plain-written) row
// reports merge.VersionLegacy; mixing plain writes and CAS on one key is
// unsupported.
func (s *Storage) GetWithVersion(key string) (io.Reader, uint64, bool, error) {
	vm, hasPrev, err := s.readRowForCAS(keys.MakeMetadataKey(key))
	if err != nil {
		return nil, 0, false, mapRocksDBError("GetWithVersion", key, err)
	}
	state, cur := casClassify(vm, hasPrev)
	if state != casLive {
		token, err := s.absenceToken(state, cur)
		if err != nil {
			return nil, 0, false, storageErrors.NewInternalError("GetWithVersion", err)
		}
		return nil, token, false, nil
	}
	version := cur

	// Refresh LRU recency exactly as Get does — a CAS read is still a read, and
	// a key read only via GetWithVersion must not be treated as cold and evicted
	// while in active use. Only present under LRU with a disk cap; nil otherwise.
	if s.accessUpdater != nil {
		s.accessUpdater.UpdateNow(key)
	}

	// Build the reader from the SAME ValueMessage the version came from. This
	// deliberately does not reuse plain Get (a second, divergent metadata read);
	// it also forgoes Get's dangling-raw-file self-heal — a CAS caller instead
	// gets a retryable error and re-reads.
	var reader io.Reader
	switch vm.ValueType {
	case pb.ValueType_INLINE:
		reader = bytes.NewReader(vm.Data)
	case pb.ValueType_SEGMENT:
		r, rerr := s.segmentManager.ReadEntry(key, vm.SegmentPath, vm.SegmentOffset, vm.ValueLength)
		if rerr != nil {
			return nil, 0, false, storageErrors.NewIORetryableError("GetWithVersion", key, rerr)
		}
		if r == nil {
			return nil, 0, false, nil
		}
		reader = r
	case pb.ValueType_RAW_FILE:
		r, rerr := s.fileManager.Read(vm.RawFilePath, vm.ValueLength)
		if rerr != nil {
			if rerr == files.ErrFileLocked {
				return nil, 0, false, storageErrors.NewLockError("GetWithVersion", key, rerr)
			}
			return nil, 0, false, storageErrors.NewIORetryableError("GetWithVersion", key, rerr)
		}
		if r == nil {
			return nil, 0, false, nil
		}
		reader = r
	default:
		return nil, 0, false, storageErrors.NewCorruptionError("GetWithVersion", key, fmt.Errorf("unknown value type: %d", vm.ValueType))
	}
	return reader, version, true, nil
}

// casRowState classifies a read row for the CAS entry points (issue #267).
type casRowState int

const (
	casAbsent casRowState = iota // no row, or a TTL-expired one: absent with no history
	casFenced                    // a stamped tombstone: absent, fenced by the delete's stamp
	casLive
)

// casClassify maps a read row to its CAS state and the version that goes with
// it: a live row's effective version, a tombstone's fence stamp, or 0 when
// absent. A TTL-expired but unswept row is absent: expiry is unfenced
// (recreating naturally expired data invalidates nothing), matching what the
// plain read path reports. An unstamped tombstone (the no-base sentinel, or a
// dangling-file purge) is plain absence too. The merge operator is clock-blind,
// so the writers translate an expired row's precondition to its real stored
// version before the merge (see PutIfVersion / DeleteIfVersion).
func casClassify(vm *pb.ValueMessage, found bool) (casRowState, uint64) {
	if !found {
		return casAbsent, 0
	}
	if vm.Expiry == merge.TombstoneExpiry {
		if vm.Version == 0 {
			return casAbsent, 0
		}
		return casFenced, vm.Version
	}
	if vm.Expiry > 0 && time.Now().Unix() >= vm.Expiry {
		return casAbsent, 0
	}
	return casLive, merge.EffectiveRowVersion(vm)
}

// casAdmits is the storage-side copy of the merge operator's admission rule,
// used to fast-fail before the merge (an optimization, not the correctness
// mechanism): a live row needs an exact match; a fence admits expected == 0 or
// an observation at or after the fence's stamp (merge.fenceAdmits); absence
// admits anything, there being no history to order against.
func casAdmits(state casRowState, cur, expected uint64) bool {
	switch state {
	case casLive:
		return cur == expected
	case casFenced:
		return expected == 0 || expected >= cur
	default:
		return true
	}
}

// absenceToken is the version an absent read hands out (issue #267): the fence
// stamp while the key is dead-but-fenced, otherwise a freshly issued stamp —
// the caller's "I observed absence now". Either token, passed back as
// expected, orders a later put against every CAS delete: one stamped after the
// observation rejects it. 0 is never handed out, so a caller that echoes what
// it read always orders itself; the unordered put-if-absent is an explicit 0.
func (s *Storage) absenceToken(state casRowState, fence uint64) (uint64, error) {
	if state == casFenced {
		return fence, nil
	}
	return s.nextVersion()
}

// currentVersionOf maps a read-back row to the version a mismatch reports: 0
// for absent, the fence stamp for a tombstone, the effective version otherwise
// — always something the caller can re-read against or retry with.
func currentVersionOf(vm *pb.ValueMessage, found bool) uint64 {
	_, v := casClassify(vm, found)
	return v
}

// PutIfVersion writes the value only if the key admits expected: a live key
// needs an exact version match; an absent key admits 0 (put-if-absent) or an
// observation token from GetWithVersion, which a CAS delete stamped after that
// observation rejects (issue #267). On success it returns the new version; on
// a lost race it returns *storageErrors.VersionMismatchError carrying the
// current version — for a fenced key, the fence stamp to refetch and retry
// with. Plain writes keep last-write-wins semantics and are unaffected.
func (s *Storage) PutIfVersion(key string, body io.Reader, ttl int, expected uint64) (newVersion uint64, retErr error) {
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("cas_put", storageType).Observe(float64(time.Since(start).Milliseconds()))
		metrics.StorageOperations.WithLabelValues("cas_put", storageType, casStatus(retErr)).Inc()
	}()

	metaKey := keys.MakeMetadataKey(key)

	// Fast-fail before reading the body or spilling anything. This is an
	// optimization, not the correctness mechanism (the merge resolves the
	// authoritative compare), but it is a SOUND one: stamps are unique and
	// never reissued, so a version that mismatches now can never come to match
	// later — only the reverse race (match now, mismatch at merge) is possible,
	// and the merge handles that.
	prev, hasPrev, err := s.readRowForCAS(metaKey)
	if err != nil {
		return 0, mapRocksDBError("PutIfVersion", key, err)
	}
	// One admission rule covers put-if-absent, a guarded update, and a put
	// ordered against a fence: see casAdmits.
	if state, cur := casClassify(prev, hasPrev); !casAdmits(state, cur, expected) {
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, cur)
	}

	// Read the body exactly as Put does: up to threshold+1 bytes decides
	// inline vs raw-file.
	firstReadSize := s.inlineThreshold + 1
	if firstReadSize <= 0 {
		firstReadSize = 1
	}
	firstChunk, release := bufferpool.AcquireBuffer(firstReadSize)
	defer release()
	n, err := io.ReadFull(body, firstChunk)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, storageErrors.NewIOError("PutIfVersion", key, err)
	}

	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	}

	newStamp, err := s.nextVersion()
	if err != nil {
		return 0, storageErrors.NewInternalError("PutIfVersion", err)
	}
	operand := &pb.ValueMessage{
		Expiry:             expiry,
		Version:            newStamp,
		OpType:             pb.MetaOp_META_OP_CAS_PUT,
		CasExpectedVersion: expected,
	}

	spilledPath := ""
	if n > s.inlineThreshold {
		storageType = "raw_file"
		reader := joinPrefix(firstChunk[:n], body)
		filePath, checksum, bytesWritten, err := s.fileManager.Write(key, reader)
		if err != nil {
			if isNoSpaceError(err) {
				return 0, storageErrors.NewStorageFullError("PutIfVersion", err)
			}
			if os.IsNotExist(err) || os.IsPermission(err) {
				return 0, storageErrors.NewIORetryableError("PutIfVersion", key, err)
			}
			return 0, storageErrors.NewIOError("PutIfVersion", key, err)
		}
		spilledPath = filePath
		// Unreferenced until the merge lands (or the spill is queued for
		// reclaim); keep the orphan sweep off it meanwhile (#156).
		s.inflightRaw.Store(filePath, struct{}{})
		defer s.inflightRaw.Delete(filePath)
		operand.ValueType = pb.ValueType_RAW_FILE
		operand.RawFilePath = filePath
		operand.ValueLength = bytesWritten
		operand.Checksum = checksum
	} else {
		storageType = "inline"
		operand.ValueType = pb.ValueType_INLINE
		operand.Data = firstChunk[:n]
		operand.ValueLength = int64(n)
	}

	// Refresh the previous-row view immediately before the merge. The initial
	// pre-read can be stale by the whole body-read/spill duration, during which
	// (a) another writer may have moved the version (fail fast, reclaim the
	// spill), or (b) compaction/recompaction may have migrated the row while
	// preserving its version — the CAS still wins then, and the reclamation
	// below must credit the MIGRATED location (segment), not the stale raw
	// path. Narrowing the read-to-merge gap to microseconds makes a migration
	// inside it vanishingly rare; the residual is the same self-healing class
	// as the ambiguous read-back below.
	prev, hasPrev, err = s.readRowForCAS(metaKey)
	if err != nil {
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, mapRocksDBError("PutIfVersion", key, err)
	}
	state, cur := casClassify(prev, hasPrev)
	if !casAdmits(state, cur, expected) {
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, cur)
	}

	// Recreating over a TTL-expired-but-unswept LIVE row: the read classified
	// it absent, but the merge operator is clock-blind and sees a live row
	// with its real stored version. Match that version so the operand is not
	// dropped. A fence that lands between this read and the merge still wins:
	// the translated version predates it. Tombstones — stamped or not — and
	// missing rows need no translation, and must not get one: the operator's
	// fence rule takes the caller's token as is, and rewriting it to 0 would
	// opt the caller out of ordering against a fence that lands in between.
	if state == casAbsent && hasPrev && prev.Expiry != merge.TombstoneExpiry {
		operand.CasExpectedVersion = merge.EffectiveRowVersion(prev)
	}

	operandBytes, err := proto.Marshal(operand)
	if err != nil {
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, storageErrors.NewInternalError("PutIfVersion", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := s.meta.Handle().Merge(wo, metaKey, operandBytes); err != nil {
		// The operand never landed; a spilled file is a plain orphan to reclaim.
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, mapRocksDBError("PutIfVersion", key, err)
	}

	// Read-your-writes tells us who won. A few retries resolve a transient read
	// glitch so a losing spill is reclaimed below rather than leaked.
	got, gotFound, err := s.readRowForCASRetry(metaKey)
	if err != nil {
		// The merge committed but its outcome is unreadable right now (a transient
		// read glitch, or a genuinely unhealthy DB). We must NOT delete the spill:
		// if the CAS won, the committed row references it, and deleting it would
		// leave a live row pointing at a missing file — silent data loss. Instead
		// make the spill discoverable so its fate is resolved once the DB reads
		// cleanly, reusing the SAME compaction-index machinery a plain Put and a
		// won CAS put (finishWonCASPut) use for the medium band: the compactor
		// validates each entry against current metadata and either migrates the
		// file (we won) or drops the entry and queues the file for deletion (we
		// lost). The merge just succeeded, so this write almost always lands even
		// while the read glitches.
		if spilledPath != "" {
			if operand.ValueLength > int64(s.inlineThreshold) && operand.ValueLength <= s.compactThreshold {
				cIdxKey, cIdxVal := compaction.PrepareEntryForCompaction(key, spilledPath)
				bwo := grocksdb.NewDefaultWriteOptions()
				if perr := s.meta.Handle().Put(bwo, cIdxKey, cIdxVal); perr != nil {
					zlog.Error().Err(perr).Str("key", key).Str("file", spilledPath).
						Msg("storage.PutIfVersion: failed to record spill recovery breadcrumb after read-back failure")
				}
				bwo.Destroy()
			} else {
				// Large spill: no write path indexes large raw files (they stay
				// raw, never compacted), so there is no existing machinery to
				// reclaim one orphaned by a lost CAS whose read-back failed. This
				// is the same orphaned-large-file class as a large plain Put that
				// fails to commit — issue #156 (reference-safe reclaim built once
				// for every write path), not CAS-specific machinery here.
				zlog.Warn().Str("key", key).Str("file", spilledPath).
					Msg("storage.PutIfVersion: read-back failed after commit; large spill may orphan if the CAS lost (issue #156)")
			}
		}
		// prev, unlike our spill, IS safe to reclaim: it is dead in every outcome
		// — we won (the row is now our value), we lost (the winner holds a fresh
		// path and already reclaimed it), or the row vanished first. Raw paths
		// are unique UUIDs, so no live row can reference prev after our merge.
		// On the dominant won-but-glitched case this keeps prev's (potentially
		// 256 MB) file out of the permanent #156 orphan class; on the rare lost
		// case it is a bounded, self-healing duplicate. The win-only bookkeeping
		// (eviction index, the size delta for our own value) is left to the
		// startup/hourly reconcile, since whether our value landed is unknown.
		if hasPrev {
			s.reclaimReplacedValue(prev)
		}
		return 0, mapRocksDBError("PutIfVersion", key, err)
	}
	if !gotFound || got.Version != newStamp {
		// We did not win. Reclaim only our own spill — never prev. Attributing
		// prev's reclamation to a loser is unsound under contention: N losers
		// would each credit the same replaced segment N times (a delete-index
		// counter, not an idempotent flag), inflating dead-byte accounting and
		// triggering premature recompaction. prev belongs to whoever actually
		// replaced it (the winner, in finishWonCASPut) or, in the rare
		// won-then-immediately-overwritten case, to the walk-gated recompactor
		// (segment dead space) and the hourly size reconcile.
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, currentVersionOf(got, gotFound))
	}

	// Won. Publish the surrounding bookkeeping that putLow does in-batch for
	// plain writes. It cannot go in the merge batch — we did not yet know the
	// outcome — so it lands in a follow-up batch. Crash windows here are the
	// self-healing kind: a missing eviction-index entry is backfilled by the
	// startup/hourly reconcile (#189/#209), and a missing compaction-index row
	// only delays raw→segment migration.
	s.finishWonCASPut(key, metaKey, operand, prev, hasPrev)

	// Outcome (success/mismatch/error) is recorded centrally by the deferred
	// recorder; only the bytes-written gauge is specific to the success path.
	metrics.StorageBytes.WithLabelValues("cas_put", storageType).Add(float64(operand.ValueLength))
	return newStamp, nil
}

// reclaimReplacedValue releases the backing bytes of a value this CAS
// replaced: a segment copy is credited to the delete index (the recompactor's
// evidence the bytes are dead) and a raw file is queued for deletion. Safe to
// call when another writer may have already reclaimed the same value: a
// duplicate queue entry is a no-op and a duplicate segment credit only
// advances recompaction eligibility, which validates liveness before acting.
func (s *Storage) reclaimReplacedValue(prev *pb.ValueMessage) {
	switch {
	case prev.ValueType == pb.ValueType_SEGMENT && prev.SegmentPath != "":
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := s.meta.Handle().Merge(wo, keys.MakeDeleteIndexKey(prev.SegmentPath), merge.MakeDeleteIndexOperand(1, prev.ValueLength)); err != nil {
			zlog.Error().Err(err).Str("segment", prev.SegmentPath).Msg("storage: failed to credit replaced segment bytes")
		}
	case prev.ValueType == pb.ValueType_RAW_FILE && prev.RawFilePath != "":
		s.stageRawFileDeletion(prev.RawFilePath)
	}
}

// finishWonCASPut applies the bookkeeping a plain put does in-batch, after a
// CAS put has been confirmed the winner: eviction-index entries, the
// compaction-index row for medium raw files, replaced-value reclamation, and
// size accounting.
func (s *Storage) finishWonCASPut(key string, metaKey []byte, newVM *pb.ValueMessage, prev *pb.ValueMessage, hasPrev bool) {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Medium raw files are eligible for background compaction into segments.
	if newVM.ValueType == pb.ValueType_RAW_FILE &&
		newVM.ValueLength > int64(s.inlineThreshold) && newVM.ValueLength <= s.compactThreshold {
		cIdxKey, cIdxVal := compaction.PrepareEntryForCompaction(key, newVM.RawFilePath)
		batch.Put(cIdxKey, cIdxVal)
	}

	// Eviction indexing, mirroring putLow.
	if s.cleaner.maxDiskUsage > 0 {
		now := time.Now()
		switch s.evictionPolicy {
		case EvictionPolicyFIFO:
			s.writeFifoIndexEntry(batch, key, now)
		default: // LRU
			accessKey := keys.MakeBucketedAccessKey(key, now)
			batch.Put(accessKey, []byte{})
			batch.Put(keys.MakeBucketedAccessIndexKey(key), accessKey)
		}
	}

	if batch.Count() > 0 {
		if err := s.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.PutIfVersion: bookkeeping batch failed; indexes will self-heal")
		}
	}

	// The replaced value's backing bytes are unreachable now that we won: the
	// base the merge matched is the row the immediately-preceding refresh read
	// observed (the version could not have changed in between without the CAS
	// losing, and stamps are never reissued). Crash windows between the merge
	// and this reclamation are the self-healing kind (duplicate-safe queue,
	// walk-validated recompaction, hourly size reconcile).
	prevSize := int64(0)
	if hasPrev {
		prevSize = prev.ValueLength
		s.reclaimReplacedValue(prev)
	}

	s.notifyPut(newVM.ValueLength - prevSize)
}

// DeleteIfVersion deletes the key only if it admits expected: a live key needs
// an exact version match; an absent or dead key admits 0 (delete-if-absent) or
// a token at or after its fence. The delete lands as a REF-LESS already-expired
// tombstone stamped with this delete's version (the merge operator drops the
// backing references so an in-flight compaction operand cannot resurrect it).
// That stamp is the key's FENCE (issue #267): until the cleaner ages it out
// (fence retention), GetWithVersion reports it as the absent key's token and a
// put carrying an older observation is rejected. Delete-if-absent on a missing
// or dead key therefore is NOT a no-op: it records the delete as a fence, or
// moves an existing fence forward, so a populate that observed absence before
// this delete loses to it.
//
// Because the tombstone carries no references, this method reclaims the
// replaced value's backing bytes itself on a confirmed win and drops the key's
// eviction-index entries; the cleaner only removes the tiny leftover row once
// the fence has aged out. This frees the (potentially 256 MB) backing file
// immediately rather than one cleanup interval later.
func (s *Storage) DeleteIfVersion(key string, expected uint64) (retErr error) {
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("cas_delete", "unknown").Observe(float64(time.Since(start).Milliseconds()))
		metrics.StorageOperations.WithLabelValues("cas_delete", "unknown", casStatus(retErr)).Inc()
	}()

	metaKey := keys.MakeMetadataKey(key)

	// Refresh immediately before the merge: this is both the fast-fail check and
	// the source of the reclaim target. Reading here (rather than earlier)
	// narrows the window in which a compaction could migrate the row between our
	// read and the merge to microseconds; the ref-less tombstone means we, not
	// the cleaner, must reclaim what this prev references.
	prev, hasPrev, err := s.readRowForCAS(metaKey)
	if err != nil {
		return mapRocksDBError("DeleteIfVersion", key, err)
	}

	// Admission (see casAdmits), with one deliberate narrowing: on a plainly
	// absent key (missing, or TTL-expired-but-unswept) only delete-if-absent
	// (expected == 0) proceeds — there is no history a token could match — and
	// it proceeds to the merge rather than returning early, because recording
	// the delete IS its job (the fence, issue #267). On a fenced key, 0 or a
	// token at or after the fence moves the fence forward; on a live key the
	// version must match exactly.
	state, cur := casClassify(prev, hasPrev)
	switch state {
	case casLive:
		if cur != expected {
			return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, cur)
		}
	case casFenced:
		if !casAdmits(state, cur, expected) {
			return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, cur)
		}
	default:
		if expected != 0 {
			return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, 0)
		}
	}

	// Capture the eviction-index entry that belongs to the value being
	// deleted, BEFORE the merge: it is the only entry this delete may remove
	// (see the win path below).
	deadEntry := s.evictionEntryFor(key)

	newStamp, err := s.nextVersion()
	if err != nil {
		return storageErrors.NewInternalError("DeleteIfVersion", err)
	}
	operand := &pb.ValueMessage{
		Version:            newStamp,
		OpType:             pb.MetaOp_META_OP_CAS_DELETE,
		CasExpectedVersion: expected,
	}
	// Delete-if-absent over a TTL-expired-but-unswept LIVE row: the clock-blind
	// operator sees a row with its real stored version, so match that version
	// to tombstone it — and since the result is ref-less, this delete then owns
	// the reclaim of the expired row's bytes. An unstamped tombstone needs no
	// translation: expected is already 0 here, and the fence rule admits it.
	if state == casAbsent && hasPrev && prev.Expiry != merge.TombstoneExpiry {
		operand.CasExpectedVersion = merge.EffectiveRowVersion(prev)
	}
	operandBytes, err := proto.Marshal(operand)
	if err != nil {
		return storageErrors.NewInternalError("DeleteIfVersion", err)
	}

	// Every eviction-index entry of the value being deleted — the captured
	// one, and any the asynchronous LRU refresh flushes for a read that
	// preceded the delete — carries a write time before this instant; a
	// recreate's entry, written after the merge commits, carries one after it.
	// The win path uses this cutoff to tell the two apart.
	cutoff := time.Now()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := s.meta.Handle().Merge(wo, metaKey, operandBytes); err != nil {
		return mapRocksDBError("DeleteIfVersion", key, err)
	}

	// Read-your-writes with retry: the merge already committed, so a transient
	// read glitch here must not turn a confirmed win into an unknown outcome — on
	// a win it is this read-back that gates the immediate reclaim of the replaced
	// value's (potentially 256 MB) backing bytes.
	got, gotFound, err := s.readRowForCASRetry(metaKey)
	if err != nil {
		// The merge committed but the outcome is unreadable. prev's BYTES are
		// dead in EVERY outcome — we won (the row is a ref-less tombstone), we
		// lost to a newer write (the winner holds a fresh path and already
		// reclaimed prev), or the row vanished first (whoever removed it
		// reclaimed prev) — so reclaiming them is never wrong: a duplicated raw
		// delete is an exact no-op and a segment credit is liveness-validated.
		// Raw paths are unique UUIDs, so no live row can reference prev after our
		// merge. On the dominant won-but-glitched case this is the only thing
		// standing between prev's (potentially 256 MB) file and a permanent #156
		// orphan.
		//
		// The SIZE accounting, however, is outcome-dependent and must NOT be
		// applied here: on a loss the displacing writer already subtracted prev,
		// so a second notifyDelete would under-report usage and could let the
		// disk cap over-admit. Leaving it to the startup/hourly reconcile means
		// the worst case on a win is a temporary over-report — the safe
		// direction. (Same split as PutIfVersion's read-back-failure path.)
		if hasPrev {
			s.reclaimReplacedValue(prev)
		}
		return mapRocksDBError("DeleteIfVersion", key, err)
	}
	switch {
	case !gotFound:
		// The row is gone entirely, which is NOT proof our precondition held: the
		// dominant cause is that a concurrent write moved the version (dropping our
		// operand at the merge) and an independent delete then removed the row, so
		// our guarded delete never applied. Report a mismatch (current version 0)
		// rather than a false success — the caller re-reads and sees the key is
		// already absent. (The inverse, a genuine win whose ref-less tombstone was
		// swept by the cleaner in the microseconds before this read-back, is
		// vanishingly rare; its only cost is a delayed reclaim of the replaced
		// bytes, recovered by the recompactor / hourly size reconcile — far better
		// than reporting success for a delete that did not happen.)
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, 0)
	case got.Version == newStamp:
		// Won: our stamp is on the tombstone. Reclaim the replaced value's
		// backing bytes now (the tombstone is ref-less, so the cleaner will not)
		// and account for the freed bytes. Two bounded, self-healing residuals:
		// a crash between the merge and this reclaim leaks the backing file (the
		// #156 orphan class); and if compaction migrated prev in the microsecond
		// between the pre-merge read and the merge, we reclaim the pre-migration
		// location — a stale raw-file delete is a no-op, and the migrated
		// segment's dead bytes are recovered by the walk-gated recompactor. Both
		// are the price of the ref-less tombstone that stops resurrection.
		if hasPrev {
			s.reclaimReplacedValue(prev)
			s.notifyDelete(prev.ValueLength)
		}
		// The key is dead and its tombstone is retained as a fence for hours
		// (issue #267), so the dead value's eviction-index generation would
		// otherwise sit at the head of the eviction order, skipped with a point
		// read on every pass. Drop that generation, and nothing that could
		// belong to a recreate.
		s.dropDeadEvictionGeneration(key, cutoff, deadEntry)
		return nil
	default:
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, currentVersionOf(got, gotFound))
	}
}

// evictionEntryFor returns a copy of the ordered eviction-index entry key that
// key's back-reference currently points at, or nil when there is none (no
// disk cap, or an unindexed key). Policy-aware, like stageEvictionIndexDeletes.
func (s *Storage) evictionEntryFor(key string) []byte {
	var backref []byte
	if s.evictionPolicy == EvictionPolicyFIFO {
		backref = keys.MakeFifoBackrefKey(key)
	} else {
		backref = keys.MakeBucketedAccessIndexKey(key)
	}
	slice, err := s.meta.Handle().Get(putPointReadOpts, backref)
	if err != nil {
		return nil
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil
	}
	return append([]byte(nil), slice.Data()...)
}

// dropDeadEvictionGeneration removes the eviction-index generation of a value
// a confirmed CAS delete replaced, deciding by the ENTRY'S WRITE TIME, which
// every index entry key embeds. Entries of the dead value — the one captured
// before the merge, and any the asynchronous LRU refresh flushed for a read
// that preceded the delete — were written before cutoff (the instant before
// the merge); a recreate's entry, written after the merge committed, was
// written after it. So the entry the back-reference currently points at is
// dropped, together with the back-reference, only if it is older than cutoff;
// a recreate's generation is never touched, and no atomicity between reading
// the row and writing the deletes is needed. The captured entry is deleted
// regardless: no other value can share its key.
//
// Two residuals, both chosen over the alternative. A back-reference rewritten
// by a recreate between the read here and the batch write: the recreate keeps
// its entry (eviction treats an entry with no back-reference as
// authoritative, so the key stays evictable) and the next reconcile's coverage
// backfill restores a back-reference. And a read that fetched the live row
// before the merge but reached its access refresh after cutoff: its refreshed
// entry looks newer than cutoff and is left for the sweep to drop with the
// tombstone, costing eviction one skipped point read per pass until then.
// Taking cutoff after the merge, or re-checking the row, would trade that
// efficiency residual for a coverage one — a recreate's generation deleted
// until the hourly backfill — which is the wrong side for the disk cap.
// Coverage is never lost. A backward wall-clock step could make a recreate's
// entry look older than cutoff; that is the same clock residual fence
// retention has, and it self-heals the same way.
func (s *Storage) dropDeadEvictionGeneration(key string, cutoff time.Time, deadEntry []byte) {
	var backref []byte
	if s.evictionPolicy == EvictionPolicyFIFO {
		backref = keys.MakeFifoBackrefKey(key)
	} else {
		backref = keys.MakeBucketedAccessIndexKey(key)
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	if deadEntry != nil {
		batch.Delete(deadEntry)
	}
	if cur, err := s.meta.Handle().Get(putPointReadOpts, backref); err == nil {
		if cur.Exists() {
			if written, ok := evictionEntryTime(cur.Data()); ok && written.Before(cutoff) {
				batch.Delete(cur.Data())
				batch.Delete(backref)
			}
		}
		cur.Free()
	}
	if batch.Count() == 0 {
		return
	}
	if err := s.meta.Handle().Write(wo, batch); err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.DeleteIfVersion: dropping the dead value's eviction generation failed; eviction skips it until the fence is swept")
	}
}

// evictionEntryTime returns the write time embedded in an ordered
// eviction-index entry key of either policy.
func evictionEntryTime(entry []byte) (time.Time, bool) {
	if keys.IsBucketedAccessKey(entry) {
		_, at, err := keys.ParseBucketedAccessKey(entry)
		return at, err == nil
	}
	at, err := keys.ParseFifoIndexTime(entry)
	return at, err == nil
}
