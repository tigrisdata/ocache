// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package merge

import (
	"encoding/binary"

	pb "github.com/tigrisdata/ocache/storage/proto"

	"google.golang.org/protobuf/proto"
)

// MakeDeleteIndexOperand creates an operand for delete index merge operations.
// The operand contains the number of entries deleted and bytes deleted.
func MakeDeleteIndexOperand(deletedEntries, deletedBytes int64) []byte {
	operand := make([]byte, 16)
	binary.LittleEndian.PutUint64(operand[0:8], uint64(deletedEntries))
	binary.LittleEndian.PutUint64(operand[8:16], uint64(deletedBytes))
	return operand
}

// MakeRawFilePurgeOperand builds a metadata merge operand that tombstones a
// dangling raw-file reference — a metadata entry whose backing file no longer
// exists on disk (e.g. a write that did not survive an unclean shutdown).
//
// The operand is a RAW_FILE-typed ValueMessage carrying only rawFilePath, which
// mergeMetadataCAS treats as a compare-and-swap precondition: the key is
// tombstoned only if the current metadata is still a RAW_FILE entry pointing at
// this exact path. Because storage.Put always writes a fresh UUID filename, a
// path match uniquely identifies the dangling file, so a concurrent Put or
// compaction that replaced the key is never clobbered. See
// MultiplexOperator.mergeMetadataCAS for the precondition semantics.
func MakeRawFilePurgeOperand(rawFilePath string) ([]byte, error) {
	return proto.Marshal(&pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: rawFilePath,
	})
}

// Future operand creators can be added here:
//
// MakeCounterOperand creates an operand for counter increment operations
// func MakeCounterOperand(increment int64) []byte {
//     operand := make([]byte, 8)
//     binary.LittleEndian.PutUint64(operand, uint64(increment))
//     return operand
// }
//
// MakeListOperand creates an operand for list append operations
// func MakeListOperand(items [][]byte) []byte {
//     // Serialize list items
// }
