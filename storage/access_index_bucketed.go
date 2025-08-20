package storage

import (
	"encoding/binary"
	"time"

	"github.com/tigrisdata/ocache/storage/keys"
)

const (
	// BucketDuration defines the time window for each bucket (1 hour)
	BucketDuration = time.Hour
)

// TimeBucketedAccessIndex manages access times using time-bucketed keys
// for scalable LRU eviction without loading all keys into memory
type TimeBucketedAccessIndex struct {
	// No state needed - all operations are stateless
}

// MakeBucketedAccessValue creates the value for a bucketed access entry
// The value contains the size of the object (8 bytes)
func MakeBucketedAccessValue(size int64) []byte {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(size))
	return val
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
func GetOldestAccessBucketPrefix() []byte {
	return []byte(keys.AccessBucketPrefix)
}

// GetOldAccessBucketPrefixes returns a list of bucket prefixes older than the given duration
// that can be deleted during maintenance
func GetOldAccessBucketPrefixes(olderThan time.Duration) []string {
	cutoff := time.Now().Add(-olderThan)
	truncated := cutoff.Truncate(BucketDuration)

	// We'll limit to 24*30 = 720 buckets (30 days) to avoid excessive iteration
	maxBuckets := 720

	var prefixes []string
	// Generate hourly bucket prefixes going back from cutoff
	for i := 0; i < maxBuckets; i++ {
		bucket := truncated.Add(-time.Duration(i) * BucketDuration)
		if bucket.Before(time.Now().Add(-30 * 24 * time.Hour)) {
			break // Don't go back more than 30 days
		}
		prefixes = append(prefixes, keys.GetBucketedAccessKey(bucket))
	}

	return prefixes
}
