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

		// CAS precondition: must be a RAW_FILE → SEGMENT transition with a
		// non-empty expected path carried on the operand.
		if !hadBase ||
			base.ValueType != pb.ValueType_RAW_FILE ||
			op.ValueType != pb.ValueType_SEGMENT ||
			op.RawFilePath == "" ||
			base.RawFilePath != op.RawFilePath {
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
