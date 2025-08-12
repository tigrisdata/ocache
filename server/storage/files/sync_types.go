package files

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	// SyncIndexPrefix is the prefix for all sync tracking entries in RocksDB
	SyncIndexPrefix = "!sync/"
)

// SyncEntry represents a file pending sync in the sync index
type SyncEntry struct {
	MetadataKey string `json:"metadata_key"` // Key to fetch metadata for validation
	Timestamp   int64  `json:"timestamp"`    // Unix timestamp when file was written
}

// EncodeSyncEntry serializes a SyncEntry to bytes
func EncodeSyncEntry(entry *SyncEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// DecodeSyncEntry deserializes a SyncEntry from bytes
func DecodeSyncEntry(data []byte) (*SyncEntry, error) {
	var entry SyncEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// MakeSyncKey creates a sync index key for a file
func MakeSyncKey(filepath string) []byte {
	key := fmt.Sprintf("%s%020d/%s", SyncIndexPrefix, time.Now().UnixNano(), filepath)
	return []byte(key)
}

// ParseSyncKey extracts timestamp and filepath from a sync key
func ParseSyncKey(key []byte) (int64, string, error) {
	keyStr := string(key)
	if !strings.HasPrefix(keyStr, SyncIndexPrefix) {
		return 0, "", fmt.Errorf("invalid sync key prefix")
	}

	// Remove prefix
	remainder := keyStr[len(SyncIndexPrefix):]

	// Split by first slash to separate timestamp from filepath
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid sync key format")
	}

	// Parse timestamp
	var timestamp int64
	if _, err := fmt.Sscanf(parts[0], "%020d", &timestamp); err != nil {
		return 0, "", fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return timestamp, parts[1], nil
}

// IsSyncKey checks if a key is a sync index entry
func IsSyncKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SyncIndexPrefix))
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
