// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"time"

	"github.com/tigrisdata/ocache/storage/keys"
)

const (
	// BucketDuration defines the time window for each bucket (1 hour)
	BucketDuration = time.Hour
)

// TimeBucketedAccessIndex manages access times using time-bucketed keys
// for LRU eviction without loading all keys into memory
type TimeBucketedAccessIndex struct {
	// No state needed - all operations are stateless
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
