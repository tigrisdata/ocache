// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"encoding/binary"
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
// Version semantics track ROW STATE, not visibility (the operator cannot
// consult the clock): an expired-but-unswept row keeps a definite version and
// recreating over it requires that token; expected == 0 matches only a truly
// absent row. Legacy (pre-versioning) rows match merge.VersionLegacy.

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

// currentVersionOf maps a read row to the version the API reports: 0 for
// absent, merge.EffectiveVersion otherwise.
func currentVersionOf(vm *pb.ValueMessage, found bool) uint64 {
	if !found {
		return 0
	}
	return merge.EffectiveVersion(vm.Version)
}

// PutIfVersion writes the value only if the key's current version equals
// expected (0 = put-if-absent). On success it returns the new version; on a
// lost race it returns *storageErrors.VersionMismatchError carrying the
// current version. Plain writes keep last-write-wins semantics and are
// unaffected.
func (s *Storage) PutIfVersion(key string, body io.Reader, ttl int, expected uint64) (uint64, error) {
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("cas_put", storageType).Observe(float64(time.Since(start).Milliseconds()))
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
	if currentVersionOf(prev, hasPrev) != expected && expected != 0 {
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, currentVersionOf(prev, hasPrev))
	}
	if expected == 0 && hasPrev {
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, currentVersionOf(prev, hasPrev))
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
	if currentVersionOf(prev, hasPrev) != expected {
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, currentVersionOf(prev, hasPrev))
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

	// Read-your-writes tells us who won.
	got, gotFound, err := s.readRowForCAS(metaKey)
	if err != nil {
		return 0, mapRocksDBError("PutIfVersion", key, err)
	}
	if !gotFound || got.Version != newStamp {
		// Not our stamp. Two distinguishable cases:
		//
		//  - Pure loss: the row still holds exactly the version we expected —
		//    our operand was dropped and the base is untouched. Reclaim only
		//    our spill.
		//
		//  - Superseded/ambiguous: the row moved past both us and our
		//    expectation. Either we lost and the true winner already reclaimed
		//    prev, or we WON and were immediately overwritten — in which case
		//    the overwriter reclaimed OUR value and nobody reclaimed prev.
		//    The two are indistinguishable from the row alone, so reclaim
		//    prev's backing bytes here as well: if the winner already did, a
		//    duplicate raw-file queue entry is a benign no-op and a duplicate
		//    segment credit only advances recompaction eligibility (the walk
		//    validates liveness before touching anything). The size counter is
		//    left to the hourly reconcile, which recomputes from live rows.
		//
		// Either way the caller gets a mismatch, which is truthful in the
		// linearized history: at read-back, the caller's value is not the
		// current value.
		pureLoss := gotFound && merge.EffectiveVersion(got.Version) == expected
		if !pureLoss && hasPrev {
			s.reclaimReplacedValue(prev)
		}
		if spilledPath != "" {
			s.stageRawFileDeletion(spilledPath)
		}
		metrics.StorageOperations.WithLabelValues("cas_put", storageType, "mismatch").Inc()
		return 0, storageErrors.NewVersionMismatchError("PutIfVersion", key, currentVersionOf(got, gotFound))
	}

	// Won. Publish the surrounding bookkeeping that putLow does in-batch for
	// plain writes. It cannot go in the merge batch — we did not yet know the
	// outcome — so it lands in a follow-up batch. Crash windows here are the
	// self-healing kind: a missing eviction-index entry is backfilled by the
	// startup/hourly reconcile (#189/#209), and a missing compaction-index row
	// only delays raw→segment migration.
	s.finishWonCASPut(key, metaKey, operand, prev, hasPrev)

	metrics.StorageOperations.WithLabelValues("cas_put", storageType, "success").Inc()
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
// The delete lands as an already-expired tombstone (the merge operator's
// sentinel convention) carrying the value's backing references; the TTL
// cleaner reclaims the row, its backing bytes, and its eviction-index entries
// on its next sweep — exactly as it does for naturally expired rows.
func (s *Storage) DeleteIfVersion(key string, expected uint64) error {
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("cas_delete", "unknown").Observe(float64(time.Since(start).Milliseconds()))
	}()

	metaKey := keys.MakeMetadataKey(key)

	// Fast-fail: sound for the same reason as PutIfVersion (stamps are unique).
	prev, hasPrev, err := s.readRowForCAS(metaKey)
	if err != nil {
		return mapRocksDBError("DeleteIfVersion", key, err)
	}
	if !hasPrev {
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, 0)
	}
	if merge.EffectiveVersion(prev.Version) != expected {
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, merge.EffectiveVersion(prev.Version))
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
		// The row is gone entirely: either our tombstone was already swept, or
		// an independent delete removed the key in the read-back window. The
		// two are indistinguishable from the row alone; in the second case our
		// precondition may never have applied, but the end state — key absent —
		// satisfies the delete intent, so this is deliberately reported as
		// success rather than a mismatch the caller could do nothing with.
		metrics.StorageOperations.WithLabelValues("cas_delete", "unknown", "success").Inc()
		return nil
	case got.Version == newStamp:
		// Won: the tombstone carries our stamp. Size accounting and backing-file
		// reclamation are deliberately left to the TTL cleaner's sweep of the
		// expired row — decrementing here too would double-count.
		metrics.StorageOperations.WithLabelValues("cas_delete", "unknown", "success").Inc()
		return nil
	default:
		metrics.StorageOperations.WithLabelValues("cas_delete", "unknown", "mismatch").Inc()
		return storageErrors.NewVersionMismatchError("DeleteIfVersion", key, currentVersionOf(got, gotFound))
	}
}
