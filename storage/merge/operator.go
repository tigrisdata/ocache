// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package merge provides a multiplexing merge operator for RocksDB that can handle
// different merge strategies based on key prefixes.
package merge

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"

	"google.golang.org/protobuf/proto"
)

// expiredSentinel is the precomputed wire-format encoding of
// &pb.ValueMessage{Expiry: 1}. mergeMetadataCAS emits this when a Delete
// tombstones a key before the compactor's stale CAS merge lands, so
// RocksDB never sees the Status::Corruption that a (nil, false) return
// would trigger. The read path's expiry check then reports the key as
// not-found, matching the user's Delete intent.
//
// Wire encoding: field 3 (Expiry, int64/varint) — tag = (3<<3)|0 = 0x18,
// value = varint(1) = 0x01. Precomputing this byte slice lets us avoid a
// proto.Marshal call inside FullMerge, so no marshal-failure fallback can
// reintroduce the (nil, false) error path we exist to avoid. The init()
// below verifies the bytes still match the schema at package load time.
var expiredSentinel = []byte{0x18, 0x01}

func init() {
	want, err := proto.Marshal(&pb.ValueMessage{Expiry: 1})
	if err != nil || !bytes.Equal(want, expiredSentinel) {
		panic(fmt.Sprintf(
			"merge: expiredSentinel out of sync with ValueMessage schema; "+
				"precomputed=%x, proto.Marshal=%x (err=%v). "+
				"Update expiredSentinel if the ValueMessage proto changed.",
			expiredSentinel, want, err,
		))
	}
}

// VersionLegacy is the version the API reports for a LIVE entry written by a
// pre-versioning binary (stored version field 0, not a tombstone). It is
// reserved: real stamps are nanosecond-scale (see Storage.nextVersion), CAS_PUT
// strips operand fields, and no live write path ever persists version 1 — so
// matching expected == VersionLegacy against a live stored 0 is unambiguous. 0
// itself keeps meaning "absent" in CAS preconditions (put-if-absent).
const VersionLegacy uint64 = 1

// tombstoneExpiry is the sentinel expiry value (Unix epoch + 1s, always in the
// past) that marks a row as a tombstone rather than a live value — emitted by
// every merge tombstone path (purge-CAS, CAS_DELETE, and the no-base fallback).
// It can never collide with a real TTL, which is time.Now().Add(...).Unix().
const tombstoneExpiry int64 = 1

// TombstoneExpiry is the exported tombstone sentinel for the storage layer's
// read paths and the cleaner, which must recognise tombstone rows.
const TombstoneExpiry = tombstoneExpiry

// IsTombstone reports whether a stored row is a ref-less tombstone: the
// sentinel expiry with no backing location. Every tombstone producer (CAS
// delete, the read path's purge, the no-base fallback) emits exactly this
// shape, and its backing bytes were reclaimed by that producer.
func IsTombstone(vm *pb.ValueMessage) bool {
	return vm != nil && vm.Expiry == tombstoneExpiry && vm.RawFilePath == "" && vm.SegmentPath == ""
}

// EffectiveRowVersion maps a stored row to the version the API reports for
// it. A tombstone (Expiry == tombstoneExpiry) reports the stamp of the delete
// that produced it — its FENCE token (issue #267): the key is absent, but with
// history, and a put-if-absent that observed absence before that delete must
// lose to it (see the CAS_PUT rule in mergeMetadataCAS). An unstamped
// tombstone (the no-base sentinel) reports 0. A live pre-versioning row (stored
// 0) reads as VersionLegacy; any other live row reads as its stamp. Shared by
// the merge operator and the read path so the two can never disagree.
func EffectiveRowVersion(vm *pb.ValueMessage) uint64 {
	if vm == nil {
		return 0
	}
	if vm.Expiry == tombstoneExpiry {
		return vm.Version
	}
	if vm.Version == 0 {
		return VersionLegacy
	}
	return vm.Version
}

// fenceAdmits is the ordering rule for a CAS operand meeting a tombstone
// (issue #267). The tombstone's stamp is the time of the delete; the operand's
// expected version is the caller's observation token — the delete stamp it
// read, or the fresh stamp an absent read handed it. The operand applies if
// its observation is at or after the delete (expected >= stamp), or if it
// declined to order itself at all (expected == 0, "put-if-absent, don't
// care"). A token from BEFORE the delete — a populate that fetched stale bytes
// — is rejected, which is the whole point of the fence. Pure in (base,
// operand): safe to re-run at compaction.
func fenceAdmits(tombstone *pb.ValueMessage, expected uint64) bool {
	return expected == 0 || expected >= tombstone.Version
}

// MultiplexOperator is a merge operator that routes to different merge strategies
// based on key prefixes. This allows us to support multiple merge types in a single
// RocksDB instance, since RocksDB only supports one merge operator per database.
type MultiplexOperator struct {
	// Add more merge strategies here as needed in the future
	// For example: counterMerge, listAppendMerge, maxMerge, etc.
}

// NewMultiplexOperator creates a new multiplexing merge operator
func NewMultiplexOperator() *MultiplexOperator {
	return &MultiplexOperator{}
}

// Name returns the name of the merge operator
func (m *MultiplexOperator) Name() string {
	return "ocache.multiplex"
}

// FullMerge implements the merge operation, routing to different strategies based on key type
func (m *MultiplexOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	// Route based on key prefix
	switch {
	case keys.IsDeleteIndexKey(key):
		return m.mergeDeleteIndex(key, existingValue, operands)

	case keys.IsMetadataKey(key):
		return m.mergeMetadataCAS(key, existingValue, operands)

	// Add more cases here for future merge types:
	// case keys.IsCounterKey(key):
	//     return m.mergeCounter(key, existingValue, operands)
	// case keys.IsListKey(key):
	//     return m.mergeList(key, existingValue, operands)

	default:
		// For unknown key types, just use the last operand (last write wins)
		if len(operands) > 0 {
			return operands[len(operands)-1], true
		}
		return existingValue, true
	}
}

// mergeDeleteIndex handles merge operations for delete index entries
func (m *MultiplexOperator) mergeDeleteIndex(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	// Start with existing value or empty entry
	var entry pb.DeleteIndexEntry
	if len(existingValue) > 0 {
		if err := proto.Unmarshal(existingValue, &entry); err != nil {
			// If we can't unmarshal existing value, start fresh
			// but this shouldn't happen in normal operation
			entry = pb.DeleteIndexEntry{}
		}
	}

	// Apply all operands (increments)
	for _, operand := range operands {
		if len(operand) == 16 {
			// Operand format: 8 bytes for entry count + 8 bytes for byte count
			entryIncr := int64(binary.LittleEndian.Uint64(operand[0:8]))
			byteIncr := int64(binary.LittleEndian.Uint64(operand[8:16]))

			entry.DeletedEntries += entryIncr
			entry.DeletedBytes += byteIncr
		}
	}

	// Marshal the updated entry
	result, err := proto.Marshal(&entry)
	if err != nil {
		return nil, false
	}

	return result, true
}

// mergeMetadataCAS handles compare-and-swap rewrites for metadata keys, used by
// the background compactor to migrate a raw-file entry into a segment entry
// without racing against a concurrent Put on the same key.
//
// Operand encoding (see Option A convention, no dedicated operand type):
//
//   - The operand is a marshalled ValueMessage with ValueType == SEGMENT.
//   - On an operand, RawFilePath is overloaded to carry the CAS precondition:
//     the raw-file path the compactor observed when it started migrating this
//     entry. This is the only context in which a SEGMENT-typed ValueMessage
//     carries RawFilePath; stored SEGMENT values never do.
//
// CAS semantics: for each operand we apply the rewrite only when the current
// base is a RAW_FILE entry whose RawFilePath equals the operand's precondition.
// Otherwise a concurrent Put replaced the raw file between the compactor's read
// and this merge; we drop the operand and keep the existing base so the newer
// write wins. The segment bytes the compactor already wrote become dead space,
// reclaimable by the segment recompactor.
//
// Multiple operands are applied in order so that stacked compactor passes
// resolve correctly; an operand that fails its precondition is skipped without
// affecting subsequent ones.
func (m *MultiplexOperator) mergeMetadataCAS(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var base pb.ValueMessage
	if len(existingValue) > 0 {
		if err := proto.Unmarshal(existingValue, &base); err != nil {
			// Unparseable base: keep existing bytes verbatim rather than
			// silently corrupting by applying merges to a nil base.
			return existingValue, true
		}
	}

	hadBase := len(existingValue) > 0
	for _, operandBytes := range operands {
		var op pb.ValueMessage
		if err := proto.Unmarshal(operandBytes, &op); err != nil {
			continue // malformed operand — skip, keep current base
		}

		// Version-CAS (issue #254): operands tagged with an explicit MetaOp
		// carry a version precondition. Everything here is a pure function of
		// (base, operand) — FullMerge re-runs at compaction time, so consulting
		// the clock (or any external state) would make read-time and
		// compaction-time resolution diverge. The match rule uses row STATE via
		// EffectiveRowVersion: a tombstone reads as 0 ("absent"), so put-if-
		// absent (expected == 0) recreates over it and no pre-delete token can.
		// On mismatch the operand is dropped and the base kept, the same
		// convention as the path-preconditioned CAS below.
		switch op.OpType {
		case pb.MetaOp_META_OP_CAS_PUT:
			// Three bases, three rules (issue #267):
			//   - no row: nothing to order against, any expected applies (an
			//     absent read's fresh token, put-if-absent's 0, or a token
			//     whose fence has aged out — the documented horizon ABA);
			//   - a tombstone: the fence rule (fenceAdmits);
			//   - a live row: exact match on its effective version.
			var apply bool
			switch {
			case !hadBase:
				apply = true
			case base.Expiry == tombstoneExpiry:
				apply = fenceAdmits(&base, op.CasExpectedVersion)
			default:
				apply = EffectiveRowVersion(&base) == op.CasExpectedVersion
			}
			if apply {
				// Adopt the operand as the new base, stripping the
				// operand-only fields so stored values never carry them
				// (the RawFilePath-clearing pattern). Field copy, not struct
				// assignment (embedded proto lock).
				base = pb.ValueMessage{
					ValueType:     op.ValueType,
					Data:          op.Data,
					Expiry:        op.Expiry,
					RawFilePath:   op.RawFilePath,
					SegmentPath:   op.SegmentPath,
					SegmentOffset: op.SegmentOffset,
					ValueLength:   op.ValueLength,
					Checksum:      op.Checksum,
					Version:       op.Version,
				}
				hadBase = true
			}
			continue
		case pb.MetaOp_META_OP_CAS_DELETE:
			// The result of a delete that applies is always the REF-LESS
			// tombstone {Expiry: tombstoneExpiry, Version: op.Version}: no
			// ValueType/paths/length, so an in-flight compactor/recompactor
			// path-CAS operand (which matches on ValueType + path) can never
			// resurrect the key. Reclamation of the backing bytes is therefore the
			// caller's job (DeleteIfVersion reclaims on its confirmed win); the
			// stamp is both the deleter's read-back proof and the key's FENCE
			// (issue #267) until the cleaner ages it out.
			switch {
			case !hadBase:
				if op.CasExpectedVersion == 0 {
					// Delete-if-absent on a missing key records the delete anyway:
					// this is the fence for a key that is not here yet, so a
					// populate that observed absence before this delete still
					// loses to it.
					base = pb.ValueMessage{Expiry: tombstoneExpiry, Version: op.Version}
				} else {
					// The row vanished before the operand resolved: there was no
					// base to match the guard against, so this delete did NOT
					// apply to the version the caller guarded on. Emit a version-0
					// tombstone (not the operand's stamp): the read-back then sees
					// a stamp that is not its own and reports a mismatch, never a
					// false win for a precondition that never held.
					base = pb.ValueMessage{Expiry: tombstoneExpiry}
				}
				hadBase = true
			case base.Expiry == tombstoneExpiry:
				// Deleting a dead key again bumps the fence to a newer stamp, so
				// a second invalidation orders AFTER anything that observed the
				// first. Same admission rule as a put over a fence.
				if fenceAdmits(&base, op.CasExpectedVersion) {
					base = pb.ValueMessage{Expiry: tombstoneExpiry, Version: op.Version}
				}
			default:
				if EffectiveRowVersion(&base) == op.CasExpectedVersion {
					base = pb.ValueMessage{Expiry: tombstoneExpiry, Version: op.Version}
				}
			}
			continue
		}

		// Purge-CAS: a RAW_FILE-typed operand carrying a RawFilePath
		// precondition is a request from the read path (storage.Get) to
		// tombstone a dangling raw-file reference whose backing file vanished
		// after an unclean shutdown. Apply it only when the current base is
		// still that exact RAW_FILE entry; otherwise a concurrent Put or
		// compaction already replaced it and the newer state must win. Because
		// storage.Put always writes a fresh UUID filename, a path match
		// uniquely identifies the dangling file, so this can never clobber a
		// live value. On a match we tombstone via the already-expired sentinel
		// (Expiry == 1): the read path's expiry check reports the key as
		// not-found and the background cleaner sweeps the row.
		if op.ValueType == pb.ValueType_RAW_FILE && op.RawFilePath != "" {
			if hadBase &&
				base.ValueType == pb.ValueType_RAW_FILE &&
				base.RawFilePath == op.RawFilePath {
				// An UNSTAMPED tombstone: purging a dangling file is data loss,
				// not a user delete, so it must not read as a fence (issue #267)
				// — a stamped tombstone would block put-if-absent until the fence
				// aged out. With version 0 it reads as plain absence and the
				// cleaner removes it on its next pass.
				base = pb.ValueMessage{Expiry: tombstoneExpiry}
			}
			continue
		}

		// CAS precondition: a SEGMENT-typed operand with a non-empty
		// RawFilePath applies only when the base still references the path
		// the writer observed when it started migrating this entry:
		//
		//   - RAW_FILE base whose RawFilePath matches — the file compactor's
		//     raw → segment migration; or
		//   - SEGMENT base whose SegmentPath matches — the recompactor's
		//     segment → segment copy (closed segments are append-only, so a
		//     path match proves the row is unchanged since the copy's read).
		//
		// The two can never cross-match: raw files and segments live in
		// disjoint directories. On mismatch a concurrent Put replaced the
		// entry between the writer's read and this merge; the operand is
		// dropped so the newer write wins, and the bytes the writer already
		// copied become dead space the walk-gated recompactor reclaims.
		if !hadBase || op.ValueType != pb.ValueType_SEGMENT || op.RawFilePath == "" {
			continue
		}
		rawMatch := base.ValueType == pb.ValueType_RAW_FILE && base.RawFilePath == op.RawFilePath
		segMatch := base.ValueType == pb.ValueType_SEGMENT && base.SegmentPath == op.RawFilePath
		if !rawMatch && !segMatch {
			continue
		}

		// CAS matched — advance base to the new SEGMENT meta, clearing the
		// overloaded precondition field so persisted SEGMENT values remain
		// well-formed (RawFilePath always empty). Build fresh via field
		// copy; direct struct assignment would copy the embedded proto
		// lock.
		base = pb.ValueMessage{
			ValueType:     op.ValueType,
			Data:          op.Data,
			Expiry:        op.Expiry,
			SegmentPath:   op.SegmentPath,
			SegmentOffset: op.SegmentOffset,
			ValueLength:   op.ValueLength,
			Checksum:      op.Checksum,
			// Version carried over from the BASE, not the operand: storage
			// migration (raw -> segment, segment -> segment) is not a user
			// write and must never change a row's CAS token (issue #254).
			// The precondition match proves the base is the same row the
			// migrator read, so its version is authoritative.
			Version: base.Version,
			// RawFilePath intentionally omitted (CAS precondition, not a
			// live file reference).
		}
		hadBase = true
	}

	if !hadBase {
		// Reached when the meta key was deleted (or never existed) before
		// the compactor's stale merge landed — see #144's follow-up on the
		// Delete-vs-Merge race. Returning (nil, false) here is NOT safe:
		// RocksDB treats a false return from FullMerge as
		// Status::Corruption, which fails Get on the key and stalls
		// background LSM compactions that process it.
		//
		// Returning (nil, true) is also unsafe: RocksDB would store an
		// empty-bytes Put (db/merge_helper.cc: the kTypeValue branch),
		// which unmarshals to a default ValueMessage (ValueType=INLINE,
		// Data=nil) — silently resurrecting the deleted key as an empty
		// inline value.
		//
		// Instead we emit an already-expired sentinel: a ValueMessage
		// with Expiry = 1 (Unix epoch + 1s, always in the past). The read
		// path's existing expiry check (storage.Storage.Get) short-circuits
		// and returns "not found" without error, and the background
		// cleaner sweeps the sentinel row on its next pass. This cannot
		// collide with a legitimate user-set TTL, which is always computed
		// as time.Now().Add(ttl*time.Second).Unix() — billions, never 1.
		//
		// The sentinel bytes are precomputed (see expiredSentinel above) so
		// there is no proto.Marshal call here whose failure could quietly
		// fall back to the (nil, false) error path this branch exists to
		// avoid.
		return expiredSentinel, true
	}

	result, err := proto.Marshal(&base)
	if err != nil {
		return nil, false
	}
	return result, true
}

// Future merge strategies can be added as methods here:
//
// func (m *MultiplexOperator) mergeCounter(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
//     // Implement counter merge logic
// }
//
// func (m *MultiplexOperator) mergeList(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
//     // Implement list append merge logic
// }
//
// func (m *MultiplexOperator) mergeMax(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
//     // Implement max value merge logic
// }
