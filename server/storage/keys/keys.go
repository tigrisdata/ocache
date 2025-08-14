package keys

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

const (
	// MetadataPrefix is the prefix for user metadata keys stored in RocksDB.
	// This ensures user keys have their own namespace separate from internal keys.
	// Uses the same format as other internal keys (starting with !)
	MetadataPrefix = "!meta/"

	// AccessIndexPrefix is the prefix for last access time index entries
	AccessIndexPrefix = "!access/"

	// CompactionIndexPrefix is the prefix for all compaction index entries in RocksDB
	CompactionIndexPrefix = "!compact/"

	// CompactionIndexKeyFormat is the format for compaction index keys
	// Format: !compact/<timestamp>|<userKey>
	CompactionIndexKeyFormat = "!compact/%020d|%s"

	// SyncIndexPrefix is the prefix for all sync tracking entries in RocksDB
	SyncIndexPrefix = "!sync/"

	// DeletionQueuePrefix is the prefix for deletion queue entries in RocksDB
	DeletionQueuePrefix = "!del/"
)

// MakeMetadataKey creates a metadata key by adding the metadata prefix to the user key
func MakeMetadataKey(userKey string) []byte {
	return []byte(MetadataPrefix + userKey)
}

// MakeCompactionKey creates a compaction key by adding the compaction prefix to the user key
func MakeCompactionKey(ts int64, key string) []byte {
	return fmt.Appendf(nil, CompactionIndexKeyFormat, ts, key)
}

// ExtractUserKey removes the metadata prefix from a metadata key to get the original user key
func ExtractUserKey(metadataKey []byte) string {
	key := string(metadataKey)
	if strings.HasPrefix(key, MetadataPrefix) {
		return key[len(MetadataPrefix):]
	}
	return key
}

// IsMetadataKey checks if a given key is a user metadata key (has the metadata prefix)
func IsMetadataKey(key []byte) bool {
	return strings.HasPrefix(string(key), MetadataPrefix)
}

// IsInternalKey checks if a given key is an internal system key (starts with !)
func IsInternalKey(key []byte) bool {
	return len(key) > 0 && key[0] == '!'
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

// MakeDeletionQueueKey creates a deletion queue key for a file
// Format: !del/<timestamp>/<filepath>
func MakeDeletionQueueKey(timestamp int64, filepath string) []byte {
	return []byte(fmt.Sprintf("%s%020d/%s", DeletionQueuePrefix, timestamp, filepath))
}

// ParseDeletionQueueKey extracts timestamp and filepath from a deletion queue key
func ParseDeletionQueueKey(key []byte) (int64, string, error) {
	keyStr := string(key)
	if !strings.HasPrefix(keyStr, DeletionQueuePrefix) {
		return 0, "", fmt.Errorf("invalid deletion queue key prefix")
	}

	// Remove prefix
	remainder := keyStr[len(DeletionQueuePrefix):]

	// Split by first slash to separate timestamp from filepath
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid deletion queue key format")
	}

	// Parse timestamp
	var timestamp int64
	if _, err := fmt.Sscanf(parts[0], "%020d", &timestamp); err != nil {
		return 0, "", fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return timestamp, parts[1], nil
}

// IsDeletionQueueKey checks if a key is a deletion queue entry
func IsDeletionQueueKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(DeletionQueuePrefix))
}
