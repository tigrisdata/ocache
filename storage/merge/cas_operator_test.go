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

// TestMergeCAS_DeleteEmitsRefLessTombstone: a matched CAS_DELETE drops the
// base's backing references (so an in-flight compactor path-CAS cannot
// resurrect it) and reads as absent, while retaining the operand's raw stamp so
// the deleter's read-back can confirm its win.
func TestMergeCAS_DeleteEmitsRefLessTombstone(t *testing.T) {
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
	assert.Empty(t, got.RawFilePath, "tombstone must be ref-less (no resurrection via path-CAS)")
	assert.Empty(t, got.SegmentPath)
	assert.Zero(t, got.ValueLength)
	assert.Equal(t, pb.ValueType_INLINE, got.ValueType)
	assert.Equal(t, uint64(200), got.Version, "raw stamp retained for the deleter's read-back")
	assert.Zero(t, EffectiveRowVersion(got), "a tombstone reads as absent")

	// Mismatch leaves the base untouched.
	out, ok = op.FullMerge(casMetaKey, base, [][]byte{casDeleteOperand(t, 999, 200)})
	require.True(t, ok)
	assert.Zero(t, mustUnmarshal(t, out).Expiry)
}

// TestMergeCAS_RecreateOverTombstoneViaPutIfAbsent: a tombstone reads as absent,
// so only put-if-absent (expected == 0) recreates over it — a pre-delete token
// (the tombstone's own stamp) must NOT match.
func TestMergeCAS_RecreateOverTombstoneViaPutIfAbsent(t *testing.T) {
	op := NewMultiplexOperator()
	tombstone := mustMarshal(t, &pb.ValueMessage{Expiry: 1, Version: 100})

	// A stale token (the tombstone's stamp, or any prior generation) does NOT match.
	out, ok := op.FullMerge(casMetaKey, tombstone, [][]byte{casPutOperand(t, 100, 300, "stale")})
	require.True(t, ok)
	assert.Equal(t, int64(1), mustUnmarshal(t, out).Expiry, "stale token must not recreate over a tombstone")

	// put-if-absent recreates.
	out, ok = op.FullMerge(casMetaKey, tombstone, [][]byte{casPutOperand(t, 0, 200, "recreated")})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, []byte("recreated"), got.Data)
	assert.Equal(t, uint64(200), got.Version)
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
		casDeleteOperand(t, 200, 400),     // wins against v1's stamp -> ref-less tombstone
		casPutOperand(t, 0, 500, "v3"),    // wins: put-if-absent recreates over the tombstone
		casPutOperand(t, 0, 600, "never"), // loses: row is live again
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

// TestMergeCAS_TombstoneNotResurrectedByMigration is the regression for the
// resurrection bug: an in-flight compactor migration operand (SEGMENT-typed,
// RawFilePath overloaded as the precondition it read before the delete) must
// NOT revive a CAS-deleted key. The ref-less tombstone has no ValueType/path,
// so the path-CAS precondition can no longer match it.
func TestMergeCAS_TombstoneNotResurrectedByMigration(t *testing.T) {
	op := NewMultiplexOperator()
	base := mustMarshal(t, &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: "/disk/files/abc.dat",
		ValueLength: 4096,
		Version:     100,
	})

	// A client's CAS delete wins, leaving a ref-less tombstone.
	tomb, ok := op.FullMerge(casMetaKey, base, [][]byte{casDeleteOperand(t, 100, 200)})
	require.True(t, ok)

	// The compactor (which read the raw file before the delete) now lands its
	// migration operand. Pre-fix this matched the tombstone via rawMatch and
	// resurrected the row as a live SEGMENT value.
	migration := mustMarshal(t, &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		RawFilePath:   "/disk/files/abc.dat", // precondition
		SegmentPath:   "/disk/segments/seg_1.seg",
		SegmentOffset: 64,
		ValueLength:   4096,
	})
	out, ok := op.FullMerge(casMetaKey, tomb, [][]byte{migration})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, int64(1), got.Expiry, "tombstone must survive the migration operand")
	assert.NotEqual(t, pb.ValueType_SEGMENT, got.ValueType, "deleted key must not be resurrected as a live segment row")
	assert.Zero(t, EffectiveRowVersion(got), "row stays absent")
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

// TestMergeCAS_DeleteOnAbsentRowEmitsUnstampedSentinel: a CAS_DELETE whose base
// vanished (an independent delete raced ahead) did NOT match the caller's version
// precondition against anything, so it must not fake a win. The operator emits a
// version-0 tombstone (not the operand's stamp): DeleteIfVersion's read-back then
// sees a stamp that is not its own and reports a mismatch, never a false success
// for a precondition that never held. The tombstone still reads as absent, so
// put-if-absent recreates over it.
func TestMergeCAS_DeleteOnAbsentRowEmitsUnstampedSentinel(t *testing.T) {
	op := NewMultiplexOperator()

	out, ok := op.FullMerge(casMetaKey, nil, [][]byte{casDeleteOperand(t, 100, 200)})
	require.True(t, ok)
	got := mustUnmarshal(t, out)
	assert.Equal(t, int64(1), got.Expiry)
	assert.Zero(t, got.Version, "no-base delete tombstone carries no stamp, so read-back cannot mistake it for a win")
	assert.Zero(t, EffectiveRowVersion(got), "and it reads as absent")

	// put-if-absent recreates over it; the stale stamp would not.
	out2, ok := op.FullMerge(casMetaKey, out, [][]byte{casPutOperand(t, 0, 300, "recreated")})
	require.True(t, ok)
	assert.Equal(t, uint64(300), mustUnmarshal(t, out2).Version)
}
