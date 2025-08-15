package merge

import "encoding/binary"

// MakeDeleteIndexOperand creates an operand for delete index merge operations.
// The operand contains the number of entries deleted and bytes deleted.
func MakeDeleteIndexOperand(deletedEntries, deletedBytes int64) []byte {
	operand := make([]byte, 16)
	binary.LittleEndian.PutUint64(operand[0:8], uint64(deletedEntries))
	binary.LittleEndian.PutUint64(operand[8:16], uint64(deletedBytes))
	return operand
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
