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

	// AccessBucketPrefix is the prefix for time-bucketed access index entries
	// Format: !access_bucket/YYYYMMDDHH/timestamp_nano/key
	AccessBucketPrefix = "!access_bucket/"

	// AccessBucketFormat is the time format for bucket keys (YYYYMMDDHH)
	AccessBucketFormat = "2006010215"

	// AccessBucketIndexPrefix is the prefix for the secondary index mapping keys to their bucket location
	// Format: !bucket_index/<key> -> bucketed_key
	AccessBucketIndexPrefix = "!access_bucket_index/"

	// CompactionIndexPrefix is the prefix for all compaction index entries in RocksDB
	CompactionIndexPrefix = "!compact/"

	// CompactionIndexKeyFormat is the format for compaction index keys
	// Format: !compact/<timestamp>|<userKey>
	CompactionIndexKeyFormat = "!compact/%020d|%s"

	// SyncIndexPrefix is the prefix for all sync tracking entries in RocksDB
	SyncIndexPrefix = "!sync/"

	// DeletionQueuePrefix is the prefix for deletion queue entries in RocksDB
	DeletionQueuePrefix = "!del/"

	// DeleteIndexPrefix is the prefix for segment deletion tracking entries in RocksDB
	DeleteIndexPrefix = "!delete:segment/"
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

// ------------------------------
// File sync keys
// ------------------------------

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

// ------------------------------
// Deletion queue keys
// ------------------------------

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

// MakeDeleteIndexKey creates a delete index key for tracking segment deletions
func MakeDeleteIndexKey(segmentPath string) []byte {
	return []byte(DeleteIndexPrefix + segmentPath)
}

// ExtractSegmentPath extracts the segment path from a delete index key
func ExtractSegmentPath(deleteIndexKey []byte) string {
	key := string(deleteIndexKey)
	if strings.HasPrefix(key, DeleteIndexPrefix) {
		return key[len(DeleteIndexPrefix):]
	}
	return ""
}

// IsDeleteIndexKey checks if a key is a delete index entry
func IsDeleteIndexKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(DeleteIndexPrefix))
}

// ------------------------------
// LRU access keys
// ------------------------------

// GetBucketedAccessKey returns the bucket key for a given timestamp
// Format: !access_bucket/YYYYMMDDHH/
func GetBucketedAccessKey(timestamp time.Time) string {
	return fmt.Sprintf("%s%s/", AccessBucketPrefix, timestamp.Format(AccessBucketFormat))
}

// MakeBucketedAccessKey creates a bucketed access index key
// Format: !access_bucket/YYYYMMDDHH/timestamp_nano/key
func MakeBucketedAccessKey(key string, accessTime time.Time) []byte {
	bucket := GetBucketedAccessKey(accessTime)
	// Use nanoseconds for precise ordering within bucket
	nanos := accessTime.UnixNano()
	return []byte(fmt.Sprintf("%s%019d/%s", bucket, nanos, key))
}

// MakeBucketedAccessIndexKey creates a secondary index key that maps a cache key to its current bucket location
// Format: !bucket_index/<key>
func MakeBucketedAccessIndexKey(key string) []byte {
	return []byte(fmt.Sprintf("%s%s", AccessBucketIndexPrefix, key))
}

// IsBucketedAccessKey checks if a key is a bucketed access index key
func IsBucketedAccessKey(key []byte) bool {
	return len(key) >= len(AccessBucketPrefix) &&
		string(key[:len(AccessBucketPrefix)]) == AccessBucketPrefix
}

// ParseBucketedAccessKey extracts components from a bucketed access key
// Returns: original key, access time, error
func ParseBucketedAccessKey(bucketedKey []byte) (string, time.Time, error) {
	keyStr := string(bucketedKey)

	// Expected format: !access_bucket/YYYYMMDDHH/timestamp_nano/key
	prefixLen := len(AccessBucketPrefix)
	if len(keyStr) < prefixLen {
		return "", time.Time{}, fmt.Errorf("invalid bucketed key: too short")
	}

	// Skip prefix
	remaining := keyStr[prefixLen:]

	// Extract bucket (YYYYMMDDHH/)
	if len(remaining) < 11 { // 10 digits + /
		return "", time.Time{}, fmt.Errorf("invalid bucketed key: missing bucket")
	}

	// Skip bucket and slash
	remaining = remaining[11:]

	// Extract timestamp (19 digits + /)
	if len(remaining) < 20 {
		return "", time.Time{}, fmt.Errorf("invalid bucketed key: missing timestamp")
	}

	timestampStr := remaining[:19]
	var timestamp int64
	n, err := fmt.Sscanf(timestampStr, "%d", &timestamp)
	if err != nil || n != 1 {
		return "", time.Time{}, fmt.Errorf("invalid timestamp in bucketed key")
	}
	accessTime := time.Unix(0, timestamp)

	// Extract original key (everything after timestamp/)
	originalKey := remaining[20:]

	return originalKey, accessTime, nil
}

// ExtractAccessBucketFromKey extracts just the bucket portion from a full key
// Returns empty string if not a valid bucketed key
func ExtractAccessBucketFromKey(key []byte) string {
	keyStr := string(key)
	prefixLen := len(AccessBucketPrefix)

	if len(keyStr) < prefixLen+11 { // prefix + YYYYMMDDHH/
		return ""
	}

	// Return prefix + bucket + /
	return keyStr[:prefixLen+11]
}
