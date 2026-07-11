// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package merge implements a multiplexing merge operator for RocksDB that supports
// different merge strategies based on key types.
//
// # Background
//
// RocksDB only supports one merge operator per database instance. To support different
// merge strategies for different data types (counters, lists, sets, etc.), we use a
// multiplexing approach where a single merge operator routes to different merge
// implementations based on the key prefix.
//
// # Current Implementations
//
// Delete Index Merge: Atomically increments counters for tracking deleted entries in segments.
// Used by the garbage collection system to track segment fragmentation.
//
// # Adding New Merge Types
//
// To add a new merge type:
//
// 1. Add a new key prefix in the keys package (e.g., keys.CounterPrefix)
// 2. Add a detection function in keys package (e.g., keys.IsCounterKey)
// 3. Add a merge method in operator.go (e.g., mergeCounter)
// 4. Add a case in FullMerge to route to your merge method
// 5. Add an operand creator in operands.go (e.g., MakeCounterOperand)
//
// Example for adding a counter merge:
//
//	// In keys/keys.go:
//	const CounterPrefix = "!counter/"
//	func IsCounterKey(key []byte) bool {
//	    return bytes.HasPrefix(key, []byte(CounterPrefix))
//	}
//
//	// In merge/operator.go:
//	func (m *MultiplexOperator) mergeCounter(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
//	    var value int64
//	    if len(existingValue) == 8 {
//	        value = int64(binary.LittleEndian.Uint64(existingValue))
//	    }
//	    for _, op := range operands {
//	        if len(op) == 8 {
//	            value += int64(binary.LittleEndian.Uint64(op))
//	        }
//	    }
//	    result := make([]byte, 8)
//	    binary.LittleEndian.PutUint64(result, uint64(value))
//	    return result, true
//	}
//
//	// In merge/operands.go:
//	func MakeCounterOperand(increment int64) []byte {
//	    operand := make([]byte, 8)
//	    binary.LittleEndian.PutUint64(operand, uint64(increment))
//	    return operand
//	}
package merge
