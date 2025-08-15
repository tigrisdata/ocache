package storage

import (
	"encoding/binary"

	pb "github.com/tigrisdata/ocache/proto"
	
	"google.golang.org/protobuf/proto"
)

// deleteIndexMergeOperator implements a merge operator for atomic delete index updates.
// It merges delete index increments atomically, avoiding race conditions.
type deleteIndexMergeOperator struct{}

// Name returns the name of the merge operator
func (m *deleteIndexMergeOperator) Name() string {
	return "ocache.deleteIndex"
}

// FullMerge implements the merge operation for delete index updates.
// The operands contain incremental updates to be applied to the existing value.
func (m *deleteIndexMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
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

// newDeleteIndexMergeOperator creates a new merge operator for delete index
func newDeleteIndexMergeOperator() *deleteIndexMergeOperator {
	return &deleteIndexMergeOperator{}
}

// makeDeleteIndexOperand creates an operand for the merge operator
func makeDeleteIndexOperand(deletedEntries, deletedBytes int64) []byte {
	operand := make([]byte, 16)
	binary.LittleEndian.PutUint64(operand[0:8], uint64(deletedEntries))
	binary.LittleEndian.PutUint64(operand[8:16], uint64(deletedBytes))
	return operand
}