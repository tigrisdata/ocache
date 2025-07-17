package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	// AccessIndexPrefix is the prefix for last access time index entries
	AccessIndexPrefix = "!access/"
)

// PrepareAccessEntry prepares the key and value for the access time index.
// The key format is: !access/<key>
// The value is the unix timestamp as 8 bytes big-endian
func PrepareAccessEntry(key string, accessTime int64) ([]byte, []byte) {
	idxKey := fmt.Sprintf("%s%s", AccessIndexPrefix, key)

	// Store timestamp as 8 bytes big-endian
	idxVal := make([]byte, 8)
	binary.BigEndian.PutUint64(idxVal, uint64(accessTime))

	return []byte(idxKey), idxVal
}

// ParseAccessTime extracts the access time from an index value
func ParseAccessTime(value []byte) int64 {
	if len(value) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

// MakeAccessIndexKey creates an access index key for a given cache key
func MakeAccessIndexKey(key string) []byte {
	return []byte(fmt.Sprintf("%s%s", AccessIndexPrefix, key))
}

// ExtractKeyFromAccessIndex extracts the original key from an access index key
func ExtractKeyFromAccessIndex(indexKey []byte) string {
	// Remove the prefix to get the original key
	if len(indexKey) > len(AccessIndexPrefix) {
		return string(indexKey[len(AccessIndexPrefix):])
	}
	return ""
}
