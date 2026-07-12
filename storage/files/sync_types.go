// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package files

import (
	"time"

	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// EncodeSyncEntry serializes a SyncEntry to bytes
func EncodeSyncEntry(entry *pb.SyncEntry) ([]byte, error) {
	return proto.Marshal(entry)
}

// DecodeSyncEntry deserializes a SyncEntry from bytes
func DecodeSyncEntry(data []byte) (*pb.SyncEntry, error) {
	var entry pb.SyncEntry
	if err := proto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// ValidationStatus represents the status of a file during validation
type ValidationStatus int

const (
	StatusValid ValidationStatus = iota
	StatusCorrupted
	StatusStale
	StatusOrphaned
	StatusMissing
)

func (s ValidationStatus) String() string {
	switch s {
	case StatusValid:
		return "valid"
	case StatusCorrupted:
		return "corrupted"
	case StatusStale:
		return "stale"
	case StatusOrphaned:
		return "orphaned"
	case StatusMissing:
		return "missing"
	default:
		return "unknown"
	}
}

// ValidationResult contains the result of validating a sync entry
type ValidationResult struct {
	SyncKey     []byte           // The sync index key
	FilePath    string           // Path to the file
	MetadataKey string           // Metadata key if found
	Status      ValidationStatus // Validation status
	Error       error            // Any error encountered
}

// RecoveryStats tracks statistics during recovery
type RecoveryStats struct {
	Total     int
	Valid     int
	Corrupted int
	Stale     int
	Orphaned  int
	Missing   int
	Duration  time.Duration
}
