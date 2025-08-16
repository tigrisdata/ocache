// Package merge provides a multiplexing merge operator for RocksDB that can handle
// different merge strategies based on key prefixes.
package merge

import (
	"encoding/binary"

	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/keys"

	"google.golang.org/protobuf/proto"
)

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
