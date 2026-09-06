// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
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

// nextVersion issues the node-local monotonic version stamp: the wall clock in
// nanoseconds, bumped past the previously issued stamp so concurrent calls and
// clock steps can never repeat or regress a version on this node. Stamps are
// nanosecond-scale, so they can never collide with 0 ("absent") or
// merge.VersionLegacy.
func (s *Storage) nextVersion() uint64 {
	for {
		now := uint64(time.Now().UnixNano())
		last := s.lastVersion.Load()
		next := now
		if next <= last {
			next = last + 1
		}
		if s.lastVersion.CompareAndSwap(last, next) {
			return next
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

	newStamp := s.nextVersion()
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

	operandBytes, err := proto.Marshal(operand)
	if err != nil {
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
		// Lost: some other write resolved ahead of or behind us. Reclaim the
		// spill; nothing else was published.
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

	// The replaced value's backing bytes are unreachable now that we won: the
	// base the merge matched is exactly the row the pre-read observed (a win
	// with expected > 0 proves the version did not change in between, and
	// stamps are never reissued). Segment dead bytes are credited so the
	// recompactor can see them; being a follow-up batch, a crash between the
	// merge and this credit orphans them undetectably — the same hazard putLow
	// documents, accepted here because the merge outcome is not knowable
	// in-batch. Raw files are queued after this batch commits.
	prevSize := int64(0)
	if hasPrev {
		prevSize = prev.ValueLength
		if prev.ValueType == pb.ValueType_SEGMENT && prev.SegmentPath != "" {
			batch.Merge(keys.MakeDeleteIndexKey(prev.SegmentPath), merge.MakeDeleteIndexOperand(1, prev.ValueLength))
		}
	}

	if batch.Count() > 0 {
		if err := s.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.PutIfVersion: bookkeeping batch failed; indexes will self-heal")
		}
	}

	if hasPrev && prev.ValueType == pb.ValueType_RAW_FILE && prev.RawFilePath != "" {
		s.stageRawFileDeletion(prev.RawFilePath)
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

	newStamp := s.nextVersion()
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
		// a plain Delete raced ahead of the operand. Both satisfy the delete
		// intent for the observed version — success.
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
