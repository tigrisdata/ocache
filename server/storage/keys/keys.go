package keys

import "strings"

const (
	// MetadataPrefix is the prefix for user metadata keys stored in RocksDB.
	// This ensures user keys have their own namespace separate from internal keys.
	// Uses the same format as other internal keys (starting with !)
	MetadataPrefix = "!meta/"

	// AccessIndexPrefix is the prefix for last access time index entries
	AccessIndexPrefix = "!access/"
)

// MakeMetadataKey creates a metadata key by adding the metadata prefix to the user key
func MakeMetadataKey(userKey string) []byte {
	return []byte(MetadataPrefix + userKey)
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
