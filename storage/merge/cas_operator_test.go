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

func mustMarshal(t *testing.T, vm *pb.ValueMessage) []byte {
	t.Helper()
	b, err := proto.Marshal(vm)
	require.NoError(t, err)
	return b
}

func mustUnmarshal(t *testing.T, b []byte) *pb.ValueMessage {
	t.Helper()
	vm := &pb.ValueMessage{}
	require.NoError(t, proto.Unmarshal(b, vm))
	return vm
}

func casPutOperand(t *testing.T, expected, newVersion uint64, data string) []byte {
	t.Helper()
	return mustMarshal(t, &pb.ValueMessage{
		ValueType:          pb.ValueType_INLINE,
		Data:               []byte(data),
		ValueLength:        int64(len(data)),
		Version:            newVersion,
		OpType:             pb.MetaOp_META_OP_CAS_PUT,
		CasExpectedVersion: expected,
	})
}

func casDeleteOperand(t *testing.T, expected, newVersion uint64) []byte {
	t.Helper()
	return mustMarshal(t, &pb.ValueMessage{
		Version:            newVersion,
		OpType:             pb.MetaOp_META_OP_CAS_DELETE,
		CasExpectedVersion: expected,
	})
}

var casMetaKey = keys.MakeMetadataKey("cas-key")

func TestMergeCAS_PutMatchAndMismatch(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{
		ValueType: pb.ValueType_INLINE, Data: []byte("old"), ValueLength: 3, Version: 100,
	})

	// Match: expected == base version → operand adopted, op fields stripped.
	out, ok := op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 100, 200, "new")})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, []byte("new"), got.Data)
	assert.Equal(t, uint64(200), got.Version)
	assert.Equal(t, pb.MetaOp_META_OP_NONE, got.OpType, "operand-only fields must be stripped from stored values")
	assert.Zero(t, got.CasExpectedVersion)

	// Mismatch: operand dropped, base kept byte-for-byte semantics.
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 999, 200, "new")})
	require.True(t, ok)
	got = mustUnmarshal(t, out)
	assert.Equal(t, []byte("old"), got.Data)
	assert.Equal(t, uint64(100), got.Version)
}

func TestMergeCAS_PutIfAbsentAndAbsentMismatch(t *testing.T) {
	op := NewMultiplexOperator()

	// expected == 0 with no base → adopted.
	out, ok := op.FullMerge(casMetaKey, nil, [][]byte{casPutOperand(t, 0, 200, "fresh")})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, []byte("fresh"), got.Data)
	assert.Equal(t, uint64(200), got.Version)

	// expected == 0 with a live base → dropped.
	base := mustMarshal(t, &pb.ValueMessage{ValueType: pb.ValueType_INLINE, Data: []byte("x"), Version: 100})
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 0, 200, "fresh")})
	require.True(t, ok)
	assert.Equal(t, uint64(100), mustUnmarshal(t, out).Version)

	// expected != 0 with no base → lost CAS on an absent row; the established
	// no-base convention applies (expired sentinel, never corruption/resurrection).
	out, ok = op.FullMerge(casMetaKey, nil, [][]byte{casPutOperand(t, 100, 200, "stale")})
	require.True(t, ok)
	got = mustUnmarshal(t, out)
	assert.Equal(t, int64(1), got.Expiry, "no-base lost CAS must resolve to the expired sentinel")
}

func TestMergeCAS_LegacyBaseMatchesVersionLegacy(t *testing.T) {
	op := NewMultiplexOperator()
	// A pre-versioning row: no version field on disk.
	base := mustMarshal(t, &pb.ValueMessage{ValueType: pb.ValueType_INLINE, Data: []byte("legacy")})

	// expected == VersionLegacy matches a stored 0.
	out, ok := op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, VersionLegacy, 200, "upgraded")})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, []byte("upgraded"), got.Data)
	assert.Equal(t, uint64(200), got.Version)

	// expected == 0 does NOT match a legacy row (0 keeps meaning absent).
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 0, 200, "clobber")})
	require.True(t, ok)
	assert.Equal(t, []byte("legacy"), mustUnmarshal(t, out).Data)
}

func TestMergeCAS_DeleteTombstonesWithFileRefsAndNewStamp(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: "/disk/files/abc.dat",
		ValueLength: 1 << 20,
		Version:     100,
	})

	out, ok := op.FullMerge(casMetaKey, base, [][]byte{casDeleteOperand(t, 100, 200)})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, int64(1), got.Expiry, "CAS delete must tombstone via the expired sentinel")
	assert.Equal(t, "/disk/files/abc.dat", got.RawFilePath, "file refs must survive for the cleaner's reclamation")
	assert.Equal(t, int64(1<<20), got.ValueLength)
	assert.Equal(t, uint64(200), got.Version, "tombstone keeps a definite CAS token until swept")
	assert.Empty(t, got.Data)

	// Mismatch leaves the base untouched.
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casDeleteOperand(t, 999, 200)})
	require.True(t, ok)
	assert.Zero(t, mustUnmarshal(t, out).Expiry)
}

// TestMergeCAS_ExpiredBaseStillMatchesByVersion encodes the row-state (not
// visibility) rule: an expired-but-unswept row has a definite version and a
// CAS against that token succeeds — the operator is deterministic and cannot
// consult the clock.
func TestMergeCAS_ExpiredBaseStillMatchesByVersion(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{ValueType: pb.ValueType_INLINE, Expiry: 1, Version: 100})

	out, ok := op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 100, 200, "recreated")})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, []byte("recreated"), got.Data)
	assert.Equal(t, uint64(200), got.Version)

	// expected == 0 does not match a tombstoned row: it still has state.
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casPutOperand(t, 0, 300, "wrong")})
	require.True(t, ok)
	assert.Equal(t, uint64(100), mustUnmarshal(t, out).Version)
}

// TestMergeCAS_Determinism asserts read-time and compaction-time resolution
// agree: folding a prefix of operands into a new base and then applying the
// rest must produce byte-identical results to applying all operands at once.
func TestMergeCAS_Determinism(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{ValueType: pb.ValueType_INLINE, Data: []byte("v0"), Version: 100})
	operands := [][]byte{
		casPutOperand(t, 100, 200, "v1"),  // wins
		casPutOperand(t, 100, 300, "v2"),  // loses (version moved on)
		casDeleteOperand(t, 200, 400),     // wins against v1's stamp
		casPutOperand(t, 400, 500, "v3"),  // wins: recreate over the tombstone token
		casPutOperand(t, 0, 600, "never"), // loses: row has state
	}

	allAtOnce, ok := op.FullMerge(casMetaKey, base, operands)
	require.True(t, ok)

	for split := 1; split < len(operands); split++ {
		folded, ok := op.FullMerge(casMetaKey, base, operands[:split])
		require.True(t, ok)
		rest, ok := op.FullMerge(casMetaKey, folded, operands[split:])
		require.True(t, ok)
		assert.Equal(t, allAtOnce, rest, "split at %d diverged from single-pass resolution", split)
	}

	// Sanity on the final state itself.
	final := mustUnmarshal(t, allAtOnce)
	assert.Equal(t, []byte("v3"), final.Data)
	assert.Equal(t, uint64(500), final.Version)
}

// TestMergeCAS_CompactionMigrationPreservesVersion pins the invariant that
// storage migration is not a write: the compactor's raw->segment CAS rewrite
// must carry the base's version through unchanged.
func TestMergeCAS_CompactionMigrationPreservesVersion(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: "/disk/files/abc.dat",
		ValueLength: 4096,
		Version:     100,
	})
	// A compactor migration operand: SEGMENT-typed, RawFilePath overloaded as
	// the precondition, and (as the compactor does) no explicit version choice.
	migration := mustMarshal(t, &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		RawFilePath:   "/disk/files/abc.dat",
		SegmentPath:   "/disk/segments/seg_1.seg",
		SegmentOffset: 64,
		ValueLength:   4096,
	})

	out, ok := op.FullMerge(casMetaKey, base, [][]byte{migration})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, pb.ValueType_SEGMENT, got.ValueType)
	assert.Equal(t, uint64(100), got.Version, "migration must not change the CAS token")

	// And a CAS taken against the pre-migration token still succeeds after it.
	out2, ok := op.FullMerge(casMetaKey, out, [][]byte{casPutOperand(t, 100, 700, "post-migration")})
	require.True(t, ok)
	assert.Equal(t, uint64(700), mustUnmarshal(t, out2).Version)
}
