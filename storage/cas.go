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
// CAS is self-contained: it does not coordinate with the TTL cleaner's
// unconditional sweep. An expired or deleted (tombstoned) row reads as absent
// (version 0), so recreating over it is put-if-absent (expected == 0) — a
// best-effort operation with the same cleaner-race semantics as a plain Put,
// never an atomic recreate guarantee. Live legacy (pre-versioning) rows match
// merge.VersionLegacy. A CAS_DELETE reclaims the replaced value's backing bytes
// on its confirmed win, since the ref-less tombstone it leaves carries no
// references for the cleaner to act on.

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
// the post-merge read-back, where a transient read glitch (the merge already
// committed) would otherwise force an indeterminate outcome — and, for a spilled
// raw-file put, leak the spill. Resolving the outcome lets the caller reclaim a
// losing spill; only a persistent read failure (a genuinely unhealthy DB) is
// surfaced, leaving the spill as a bounded, recoverable orphan.
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
// generation's data with another's version). An absent, expired, or deleted
// (tombstoned) key reports version 0 and found == false — recreate over it with
// PutIfVersion(expected == 0). A live pre-versioning (plain-written) row reports
// merge.VersionLegacy; mixing plain writes and CAS on one key is unsupported.
func (s *Storage) GetWithVersion(key string) (io.Reader, uint64, bool, error) {
	vm, hasPrev, err := s.readRowForCAS(keys.MakeMetadataKey(key))
	if err != nil {
		return nil, 0, false, mapRocksDBError("GetWithVersion", key, err)
	}
	// Absent, or expired/tombstoned (both are logically gone) → absent.
	if !hasPrev || (vm.Expiry > 0 && time.Now().Unix() >= vm.Expiry) {
		return nil, 0, false, nil
	}
	version := merge.EffectiveRowVersion(vm)

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

// currentVersionOf maps a read row to the version the CAS match rule reports:
// 0 for absent or a tombstone, the effective version otherwise.
func currentVersionOf(vm *pb.ValueMessage, found bool) uint64 {
	if !found {
		return 0
	}
	return merge.EffectiveRowVersion(vm)
}

// casCurrentVersion is currentVersionOf plus a clock check: a TTL-expired but
// unswept row is logically absent (Get reports not-found), so it reports 0 —
// letting put-if-absent (expected == 0) recreate over it, matching what
// GetWithVersion returns for the same row. The merge operator is clock-blind, so
// PutIfVersion additionally translates the operand's precondition to the row's
// real stored version when it recreates over such a row (see PutIfVersion).
func casCurrentVersion(vm *pb.ValueMessage, found bool) uint64 {
	if !found {
		return 0
	}
	if vm.Expiry > 0 && time.Now().Unix() >= vm.Expiry {
		return 0
	}
	return merge.EffectiveRowVersion(vm)
}

// PutIfVersion writes the value only if the key's current version equals
// expected (0 = put-if-absent). On success it returns the new version; on a
// lost race it returns *storageErrors.VersionMismatchError carrying the
// current version. Plain writes keep last-write-wins semantics and are
// unaffected.
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
	// casCurrentVersion reports 0 for an absent row, a tombstone, AND a
	// TTL-expired-but-unswept row, so a single check covers put-if-absent
	// (expected == 0, matching any logically-absent key) and a guarded update
	// (expected == a live version).
	if casCurrentVersion(prev, hasPrev) != expected {
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, casCurrentVersion(prev, hasPrev))
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
	if casCurrentVersion(prev, hasPrev) != expected {
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, casCurrentVersion(prev, hasPrev))
	}

	// Recreating over a TTL-expired-but-unswept row: casCurrentVersion reported
	// it as absent (0), but the merge operator is clock-blind and sees the row's
	// real stored version. Match that version so the operand is not dropped.
	// (Tombstones need no translation — the operator already reads Expiry == 1 as
	// absent, so expected == 0 matches them directly.)
	if expected == 0 && hasPrev {
		if realVer := merge.EffectiveRowVersion(prev); realVer != 0 {
			operand.CasExpectedVersion = realVer
		}
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
		// leave a live row pointing at a missing file — silent data loss. Nor may
		// we leave it: if the CAS lost, no index reaches it and it leaks forever
		// (#156). Stage it for a REFERENCE-GUARDED deletion (works for both the
		// medium and large bands): the deletion worker keeps the file if the
		// committed row still references it (we won) and deletes it otherwise (we
		// lost), resolving whenever the DB reads cleanly. The merge just
		// succeeded, so this write almost always lands even while the read
		// glitches.
		if spilledPath != "" {
			s.stageRawFileDeletionIfUnreferenced(spilledPath, key)
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

// DeleteIfVersion deletes the key only if its current version equals expected.
// The delete lands as a REF-LESS already-expired tombstone (the merge operator
// drops the backing references so an in-flight compaction operand cannot
// resurrect it). Because the tombstone carries no references, this method
// reclaims the replaced value's backing bytes itself on a confirmed win — the
// cleaner only removes the tiny leftover row on its next sweep. This frees the
// (potentially 256 MB) backing file immediately rather than one cleanup
// interval later.
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

	// A logically-absent key — missing, tombstoned, or TTL-expired-but-unswept —
	// reports version 0, consistent with GetWithVersion, and never runs the merge
	// (a clock-blind CAS_DELETE against an expired row would loop). There is
	// nothing live to delete:
	//   - delete-if-absent (expected == 0) is already satisfied → success, no-op;
	//   - any other expected mismatches against the current version 0.
	// An unswept expired row's backing bytes are reclaimed by the TTL cleaner on
	// its next sweep, so nothing leaks.
	cur := casCurrentVersion(prev, hasPrev)
	if cur == 0 {
		if expected == 0 {
			return nil
		}
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, 0)
	}
	// Live row: match its version (cur == the physical version, since it is not
	// expired). The merge below tombstones it on a match.
	if cur != expected {
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, cur)
	}

	newStamp, err := s.nextVersion()
	if err != nil {
		return storageErrors.NewInternalError("DeleteIfVersion", err)
	}
	operand := &pb.ValueMessage{
		Version:            newStamp,
		OpType:             pb.MetaOp_META_OP_CAS_DELETE,
		CasExpectedVersion: expected,
	}
	operandBytes, err := proto.Marshal(operand)
	if err != nil {
		return storageErrors.NewInternalError("DeleteIfVersion", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := s.meta.Handle().Merge(wo, metaKey, operandBytes); err != nil {
		return mapRocksDBError("DeleteIfVersion", key, err)
	}

	got, gotFound, err := s.readRowForCAS(metaKey)
	if err != nil {
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
		return nil
	default:
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, currentVersionOf(got, gotFound))
	}
}
