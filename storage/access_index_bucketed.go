package storage

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/tigrisdata/ocache/storage/keys"
)

const (
	// BucketDuration defines the time window for each bucket (1 hour)
	BucketDuration = time.Hour

	// BucketFormat is the time format for bucket keys (YYYYMMDDHH)
	BucketFormat = "2006010215"
)

// TimeBucketedAccessIndex manages access times using time-bucketed keys
// for scalable LRU eviction without loading all keys into memory
type TimeBucketedAccessIndex struct {
	// No state needed - all operations are stateless
}

// GetBucketKey returns the bucket key for a given timestamp
// Format: !access_bucket/YYYYMMDDHH/
func GetBucketKey(timestamp time.Time) string {
	return fmt.Sprintf("%s%s/", keys.AccessBucketPrefix, timestamp.Format(BucketFormat))
}

// MakeBucketedAccessKey creates a bucketed access index key
// Format: !access_bucket/YYYYMMDDHH/timestamp_nano/key
func MakeBucketedAccessKey(key string, accessTime time.Time) []byte {
	bucket := GetBucketKey(accessTime)
	// Use nanoseconds for precise ordering within bucket
	nanos := accessTime.UnixNano()
	return []byte(fmt.Sprintf("%s%019d/%s", bucket, nanos, key))
}

// MakeBucketedAccessValue creates the value for a bucketed access entry
// The value contains the size of the object (8 bytes)
func MakeBucketedAccessValue(size int64) []byte {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(size))
	return val
}

// ParseBucketedAccessKey extracts components from a bucketed access key
// Returns: original key, access time, error
func ParseBucketedAccessKey(bucketedKey []byte) (string, time.Time, error) {
	keyStr := string(bucketedKey)

	// Expected format: !access_bucket/YYYYMMDDHH/timestamp_nano/key
	prefixLen := len(keys.AccessBucketPrefix)
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

// ParseBucketedAccessValue extracts the size from a bucketed access value
func ParseBucketedAccessValue(value []byte) int64 {
	if len(value) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

// GetOldestBucket returns the prefix for iterating the oldest bucket
// This starts from the beginning of the access index
func GetOldestBucketPrefix() []byte {
	return []byte(keys.AccessBucketPrefix)
}

// GetBucketPrefixForTime returns the prefix for a specific time's bucket
func GetBucketPrefixForTime(t time.Time) []byte {
	return []byte(GetBucketKey(t))
}

// IsAccessBucketKey checks if a key is a bucketed access index key
func IsAccessBucketKey(key []byte) bool {
	return len(key) >= len(keys.AccessBucketPrefix) &&
		string(key[:len(keys.AccessBucketPrefix)]) == keys.AccessBucketPrefix
}

// GetBucketEndKey returns the key that marks the end of a bucket
// Used for range iteration within a specific bucket
func GetBucketEndKey(bucketPrefix string) []byte {
	// Append a high character to mark the end of this bucket's range
	return []byte(bucketPrefix + "\xff")
}

// ExtractBucketFromKey extracts just the bucket portion from a full key
// Returns empty string if not a valid bucketed key
func ExtractBucketFromKey(key []byte) string {
	keyStr := string(key)
	prefixLen := len(keys.AccessBucketPrefix)

	if len(keyStr) < prefixLen+11 { // prefix + YYYYMMDDHH/
		return ""
	}

	// Return prefix + bucket + /
	return keyStr[:prefixLen+11]
}

// GetNextBucketTime returns the start time of the next bucket
func GetNextBucketTime(current time.Time) time.Time {
	return current.Truncate(BucketDuration).Add(BucketDuration)
}

// GetPreviousBucketTime returns the start time of the previous bucket
func GetPreviousBucketTime(current time.Time) time.Time {
	return current.Truncate(BucketDuration).Add(-BucketDuration)
}

// MakeBucketIndexKey creates a secondary index key that maps a cache key to its current bucket location
// Format: !bucket_index/<key>
func MakeBucketIndexKey(key string) []byte {
	return []byte(fmt.Sprintf("%s%s", keys.BucketIndexPrefix, key))
}

// CleanupOldBuckets returns a list of bucket prefixes older than the given duration
// that can be deleted during maintenance
func GetOldBucketPrefixes(olderThan time.Duration) []string {
	cutoff := time.Now().Add(-olderThan)
	truncated := cutoff.Truncate(BucketDuration)

	var prefixes []string
	// Generate hourly bucket prefixes going back from cutoff
	// We'll limit to 24*30 = 720 buckets (30 days) to avoid excessive iteration
	for i := 0; i < 720; i++ {
		bucket := truncated.Add(-time.Duration(i) * BucketDuration)
		if bucket.Before(time.Now().Add(-30 * 24 * time.Hour)) {
			break // Don't go back more than 30 days
		}
		prefixes = append(prefixes, GetBucketKey(bucket))
	}

	return prefixes
}
