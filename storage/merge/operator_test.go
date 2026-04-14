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

func TestMergeMetadataCAS_NoBase_NoApplicableOperand_ReturnsNoValue(t *testing.T) {
	// If there's no base and no operand applies (e.g., a stray merge without
	// a prior Put), return (nil, false) so RocksDB treats the key as absent.
	op := NewMultiplexOperator()
	key := metaKey(t, "user:alice")
	operand := marshal(t, segmentOperand("files/UUID-A", "segments/seg_1.seg", 128, 4096))

	_, ok := op.FullMerge(key, nil, [][]byte{operand})
	assert.False(t, ok, "merge should not synthesize a value out of thin air")
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
