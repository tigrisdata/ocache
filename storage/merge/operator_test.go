// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package merge

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// marshal is a small helper so the tests read like specs.
func marshal(t *testing.T, vm *pb.ValueMessage) []byte {
	t.Helper()
	b, err := proto.Marshal(vm)
	require.NoError(t, err)
	return b
}

func unmarshal(t *testing.T, b []byte) *pb.ValueMessage {
	t.Helper()
	vm := &pb.ValueMessage{}
	require.NoError(t, proto.Unmarshal(b, vm))
	return vm
}

// rawFile builds a stored-value ValueMessage for a raw file at the given path.
func rawFile(path string, size int64) *pb.ValueMessage {
	return &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: path,
		ValueLength: size,
	}
}

// segmentOperand builds a merge operand that migrates a raw-file entry at
// expectedPath into the given segment location. The RawFilePath on the operand
// carries the CAS precondition (see the invariant comment on ValueMessage).
func segmentOperand(expectedPath, segPath string, offset, size int64) *pb.ValueMessage {
	return &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		RawFilePath:   expectedPath, // overloaded: CAS precondition
		SegmentPath:   segPath,
		SegmentOffset: offset,
		ValueLength:   size,
	}
}

func metaKey(t *testing.T, userKey string) []byte {
	t.Helper()
	k := keys.MakeMetadataKey(userKey)
	require.True(t, keys.IsMetadataKey(k), "MakeMetadataKey must produce an IsMetadataKey-recognized key")
	return k
}

func TestMergeMetadataCAS_PreconditionMatches_AppliesRewrite(t *testing.T) {
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-A", 4096))
	operand := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_SEGMENT, got.ValueType)
	assert.Equal(t, "segments/seg_1.seg", got.SegmentPath)
	assert.Equal(t, int64(128), got.SegmentOffset)
	assert.Equal(t, int64(4096), got.ValueLength)
	assert.Empty(t, got.RawFilePath, "stored SEGMENT values must never carry RawFilePath")
}

func TestMergeMetadataCAS_PreconditionMismatches_KeepsBase(t *testing.T) {
	// Simulates the race: a concurrent Put replaced the raw file with UUID-B
	// before the compactor's merge reached this FullMerge. The operand still
	// expects UUID-A, so it must be dropped.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-B", 2048))
	operand := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_RAW_FILE, got.ValueType, "Put wins")
	assert.Equal(t, "files/UUID-B", got.RawFilePath)
	assert.Equal(t, int64(2048), got.ValueLength)
}

func TestMergeMetadataCAS_OperandWithoutPrecondition_KeepsBase(t *testing.T) {
	// An operand with empty RawFilePath cannot assert any precondition, so
	// we refuse to apply it rather than blindly overwriting.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-A", 4096))
	operand := marshal(t, &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   "segments/seg_1.seg",
		SegmentOffset: 128,
		ValueLength:   4096,
	})

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_RAW_FILE, got.ValueType)
	assert.Equal(t, "files/UUID-A", got.RawFilePath)
}

func TestMergeMetadataCAS_NonRawFileBase_KeepsBase(t *testing.T) {
	// If the base is already SEGMENT (e.g., a prior compactor pass already
	// migrated this key), the precondition cannot match — the operand is
	// dropped.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   "segments/seg_0.seg",
		SegmentOffset: 0,
		ValueLength:   4096,
	})
	operand := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_SEGMENT, got.ValueType)
	assert.Equal(t, "segments/seg_0.seg", got.SegmentPath, "existing SEGMENT base untouched")
}

func TestMergeMetadataCAS_MultipleOperands_AppliedInOrder(t *testing.T) {
	// Two stacked compactor migrations on the same key; the second one's
	// precondition must reference the first one's resolved SEGMENT, which
	// never matches (we only apply RAW_FILE → SEGMENT). So only the first
	// operand takes effect, the second is dropped. This is the expected
	// behavior — a key is compacted once; a subsequent compaction only
	// happens after a new Put reinstates a RAW_FILE base.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-A", 4096))

	first := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))
	second := marshal(t, segmentOperand("files/UUID-A", "segments/seg_2.seg", 256, 4096))

	result, ok := op.FullMerge(key, base, [][]byte{first, second})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_SEGMENT, got.ValueType)
	assert.Equal(t, "segments/seg_1.seg", got.SegmentPath,
		"first operand applies (base was RAW_FILE); second cannot because base is now SEGMENT")
	assert.Empty(t, got.RawFilePath)
}

func TestMergeMetadataCAS_NoBase_ReturnsExpiredSentinel(t *testing.T) {
	// Models the Delete-vs-Merge race (follow-up to #142/#144): a user's
	// Delete landed between the compactor's metadata validation and the
	// commit of its CAS merge. By the time FullMerge runs, the base has
	// been tombstoned (existingValue == nil) and the operand's precondition
	// cannot match.
	//
	// Returning (nil, false) here would trip RocksDB's
	// Status::Corruption path; returning (nil, true) would silently
	// resurrect the key as empty inline bytes. Instead we synthesize a
	// ValueMessage with Expiry=1 so the storage.Get expiry check treats
	// the key as not-found and the cleaner removes the sentinel later.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	operand := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))

	result, ok := op.FullMerge(key, nil, [][]byte{operand})
	require.True(t, ok, "must not return false — would cause RocksDB Status::Corruption")
	require.NotNil(t, result, "must return a sentinel so RocksDB has something to store")

	got := unmarshal(t, result)
	assert.Equal(t, int64(1), got.Expiry,
		"sentinel must carry an always-expired Expiry so the read path short-circuits")
	// The other fields stay at their zero value; it's the Expiry that makes
	// the sentinel harmless.
	assert.Empty(t, got.Data)
	assert.Empty(t, got.RawFilePath)
	assert.Empty(t, got.SegmentPath)
	assert.Zero(t, got.ValueLength)
}

func TestMergeMetadataCAS_MalformedOperand_SkippedBaseUntouched(t *testing.T) {
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-A", 4096))
	malformed := []byte{0xff, 0xff, 0xff, 0xff}

	result, ok := op.FullMerge(key, base, [][]byte{malformed})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_RAW_FILE, got.ValueType)
	assert.Equal(t, "files/UUID-A", got.RawFilePath)
}

func TestMergeMetadataCAS_PurgeMatches_Tombstones(t *testing.T) {
	// The read path observed a RAW_FILE entry whose backing file is gone
	// (issue #150) and issued a purge operand. The base still points at the
	// same dangling file, so the precondition matches and the key is
	// tombstoned via the already-expired sentinel.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-A", 32<<20))
	operand, err := MakeRawFilePurgeOperand("files/UUID-A")
	require.NoError(t, err)

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, int64(1), got.Expiry, "purge must tombstone via the always-expired sentinel")
	assert.Empty(t, got.RawFilePath)
	assert.Zero(t, got.ValueLength)
}

func TestMergeMetadataCAS_PurgeMismatch_KeepsBase(t *testing.T) {
	// A concurrent Put replaced the key with a fresh file (UUID-B) before the
	// purge operand landed. Put always writes a new path, so the precondition
	// (UUID-A) cannot match and the live value must be preserved.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, rawFile("files/UUID-B", 64<<20))
	operand, err := MakeRawFilePurgeOperand("files/UUID-A")
	require.NoError(t, err)

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_RAW_FILE, got.ValueType, "concurrent Put wins")
	assert.Equal(t, "files/UUID-B", got.RawFilePath)
	assert.NotEqual(t, int64(1), got.Expiry)
}

func TestMergeMetadataCAS_PurgeNonRawFileBase_KeepsBase(t *testing.T) {
	// A compaction migrated the key to a SEGMENT before the purge landed. The
	// purge precondition only matches a RAW_FILE base, so the now-valid
	// SEGMENT entry must be preserved.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	base := marshal(t, &pb.ValueMessage{
		ValueType:   pb.ValueType_SEGMENT,
		SegmentPath: "segments/seg_0.seg",
		ValueLength: 4096,
	})
	operand, err := MakeRawFilePurgeOperand("files/UUID-A")
	require.NoError(t, err)

	result, ok := op.FullMerge(key, base, [][]byte{operand})
	require.True(t, ok)

	got := unmarshal(t, result)
	assert.Equal(t, pb.ValueType_SEGMENT, got.ValueType, "migrated SEGMENT base untouched")
	assert.Equal(t, "segments/seg_0.seg", got.SegmentPath)
}

// Routing smoke: delete-index keys must still go through mergeDeleteIndex and
// not be mis-dispatched to mergeMetadataCAS.
func TestFullMerge_RoutingPreserved_DeleteIndex(t *testing.T) {
	op := NewMultiplexOperator()

	// A delete-index key should still use the counter-style merge.
	segmentPath := "segments/seg_1.seg"
	delKey := keys.MakeDeleteIndexKey(segmentPath)
	require.True(t, keys.IsDeleteIndexKey(delKey))

	// Two increments; expect counters to accumulate.
	result, ok := op.FullMerge(delKey, nil, [][]byte{
		MakeDeleteIndexOperand(1, 100),
		MakeDeleteIndexOperand(2, 200),
	})
	require.True(t, ok)

	var entry pb.DeleteIndexEntry
	require.NoError(t, proto.Unmarshal(result, &entry))
	assert.Equal(t, int64(3), entry.DeletedEntries)
	assert.Equal(t, int64(300), entry.DeletedBytes)
}
