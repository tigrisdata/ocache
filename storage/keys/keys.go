// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
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

	// SegmentLiveIndexPrefix is the prefix for durable segment live-location rows.
	// The row key is !segment-live/<base64url segment path>/<big-endian offset>.
	// Raw binary offsets sort in source-file order after the encoded path.
	SegmentLiveIndexPrefix = "!segment-live/"

	// SegmentLiveCoveragePrefix stores a footer fingerprint proving that all
	// records in a finalized segment have a corresponding live-location row (or
	// an intentionally absent row because the record is already dead).
	SegmentLiveCoveragePrefix = "!segment-live-covered/"

	// SegmentLiveIndexVersion versions the live-location row and coverage value
	// encodings. A mismatch makes a segment fall back to the historical scan.
	SegmentLiveIndexVersion = byte(1)

	// FifoIndexPrefix is the prefix for the FIFO eviction index. Entries embed the
	// write time in the key so the eviction scan can walk them oldest-written
	// first. Unlike the access index, entries are written once at Put and never
	// bumped on reads.
	// Format: !fifo/<write_time_nano>/<key>
	FifoIndexPrefix = "!fifo/"

	// FifoBackrefPrefix is the prefix for the FIFO secondary index that maps a
	// cache key to its current FIFO entry, mirroring the LRU access index's
	// secondary index. It lets Put/Delete/TTL locate and remove a key's previous
	// FIFO entry so the index holds exactly one entry per live key.
	// Format: !fifo_ref/<key> -> !fifo/<nano>/<key>
	FifoBackrefPrefix = "!fifo_ref/"
)

// MakeMetadataKey creates a metadata key by adding the metadata prefix to the user key
func MakeMetadataKey(userKey string) []byte {
	return []byte(MetadataPrefix + userKey)
}

// MakeCompactionKey creates a compaction key by adding the compaction prefix to the user key
func MakeCompactionKey(ts int64, key string) []byte {
	return fmt.Appendf(nil, CompactionIndexKeyFormat, ts, key)
}

// ParseCompactionIndexRow extracts userKey, filePath and size from RocksDB file-index
// key/value pairs. Returns ok=false when the row does not follow the expected
// format.
func ParseCompactionIndexRow(k, v []byte) (userKey, filePath string, ok bool) {
	// Key format: CompactionIndexKeyFormat (!compact/<ts>|<userKey>)
	pipeIdx := bytes.IndexByte(k, '|')
	if pipeIdx <= 0 {
		return
	}
	userKey = string(k[pipeIdx+1:])

	// Value format: <filePath>
	filePath = string(v)
	ok = true
	return
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

// SegmentLiveIndexEntry is the immutable information needed to reconstruct a
// segment record without reading its source header. Key is the user key; the
// remaining fields mirror the record header and are validated against metadata
// before a row is copied.
type SegmentLiveIndexEntry struct {
	Key           string
	ValueLength   int64
	HeaderSize    int64
	Checksum      uint32
	HeaderVersion uint16
}

// SegmentLiveIndexCoverage fingerprints a finalized segment footer and data
// length. A coverage row is trusted only when all fields still match the
// manager's segment, which preserves the historical scan fallback for legacy,
// damaged, or partially indexed segments.
type SegmentLiveIndexCoverage struct {
	Entries   uint32
	DataBytes int64
	Size      int64
}

// MakeSegmentLiveIndexPrefix returns the prefix for all live rows belonging to
// segmentPath. Base64 raw URL encoding keeps the filesystem path out of the
// RocksDB delimiter namespace while preserving an exact round trip.
func MakeSegmentLiveIndexPrefix(segmentPath string) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(segmentPath))
	return []byte(SegmentLiveIndexPrefix + encoded + "/")
}

// MakeSegmentLiveIndexKey creates an offset-ordered live-location row key.
// Negative offsets are invalid and return nil rather than wrapping into a
// future-sorting uint64.
func MakeSegmentLiveIndexKey(segmentPath string, offset int64) []byte {
	if segmentPath == "" || offset < 0 {
		return nil
	}
	prefix := MakeSegmentLiveIndexPrefix(segmentPath)
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(key)-8:], uint64(offset))
	return key
}

// ParseSegmentLiveIndexKey extracts a segment path and source-record offset.
func ParseSegmentLiveIndexKey(key []byte) (segmentPath string, offset int64, ok bool) {
	prefix := []byte(SegmentLiveIndexPrefix)
	if !bytes.HasPrefix(key, prefix) || len(key) < len(prefix)+1+8 {
		return "", 0, false
	}
	rest := key[len(prefix):]
	encodedLen := len(rest) - 1 - 8
	if encodedLen <= 0 || rest[encodedLen] != '/' {
		return "", 0, false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(rest[:encodedLen]))
	if err != nil || len(decoded) == 0 {
		return "", 0, false
	}
	rawOffset := binary.BigEndian.Uint64(rest[len(rest)-8:])
	if rawOffset > uint64(^uint64(0)>>1) {
		return "", 0, false
	}
	return string(decoded), int64(rawOffset), true
}

// IsSegmentLiveIndexKey reports whether key belongs to the offset row
// namespace. Coverage rows use a distinct prefix and are not included.
func IsSegmentLiveIndexKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SegmentLiveIndexPrefix))
}

// MakeSegmentLiveCoverageKey creates the marker key for a segment.
func MakeSegmentLiveCoverageKey(segmentPath string) []byte {
	if segmentPath == "" {
		return nil
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(segmentPath))
	return []byte(SegmentLiveCoveragePrefix + encoded)
}

// ParseSegmentLiveCoverageKey extracts a segment path from a marker key.
func ParseSegmentLiveCoverageKey(key []byte) (string, bool) {
	prefix := []byte(SegmentLiveCoveragePrefix)
	if !bytes.HasPrefix(key, prefix) || len(key) == len(prefix) {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(key[len(prefix):]))
	if err != nil || len(decoded) == 0 {
		return "", false
	}
	return string(decoded), true
}

// IsSegmentLiveCoverageKey reports whether key belongs to the coverage marker
// namespace.
func IsSegmentLiveCoverageKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SegmentLiveCoveragePrefix))
}

// EncodeSegmentLiveIndexEntry serializes a versioned live-location row value.
// The fixed fields avoid a protobuf/Data allocation for every index lookup.
func EncodeSegmentLiveIndexEntry(entry SegmentLiveIndexEntry) ([]byte, error) {
	if entry.Key == "" || entry.ValueLength <= 0 || entry.HeaderSize <= 0 || entry.HeaderVersion == 0 {
		return nil, fmt.Errorf("invalid segment live index entry")
	}
	if uint64(len(entry.Key)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("segment live index key is too large")
	}
	const fixed = 1 + 4 + 8 + 8 + 4 + 2
	row := make([]byte, fixed+len(entry.Key))
	row[0] = SegmentLiveIndexVersion
	binary.BigEndian.PutUint32(row[1:5], uint32(len(entry.Key)))
	pos := 5
	copy(row[pos:], entry.Key)
	pos += len(entry.Key)
	binary.BigEndian.PutUint64(row[pos:pos+8], uint64(entry.ValueLength))
	pos += 8
	binary.BigEndian.PutUint64(row[pos:pos+8], uint64(entry.HeaderSize))
	pos += 8
	binary.BigEndian.PutUint32(row[pos:pos+4], entry.Checksum)
	pos += 4
	binary.BigEndian.PutUint16(row[pos:pos+2], entry.HeaderVersion)
	return row, nil
}

// DecodeSegmentLiveIndexEntry decodes and validates a versioned row value.
func DecodeSegmentLiveIndexEntry(row []byte) (SegmentLiveIndexEntry, error) {
	const fixed = 1 + 4 + 8 + 8 + 4 + 2
	if len(row) < fixed || row[0] != SegmentLiveIndexVersion {
		return SegmentLiveIndexEntry{}, fmt.Errorf("invalid segment live index row version or length")
	}
	keyLen := int(binary.BigEndian.Uint32(row[1:5]))
	if keyLen <= 0 || fixed+keyLen != len(row) {
		return SegmentLiveIndexEntry{}, fmt.Errorf("invalid segment live index row key length")
	}
	pos := 5
	entry := SegmentLiveIndexEntry{Key: string(row[pos : pos+keyLen])}
	pos += keyLen
	valueLength := binary.BigEndian.Uint64(row[pos : pos+8])
	pos += 8
	headerSize := binary.BigEndian.Uint64(row[pos : pos+8])
	pos += 8
	entry.Checksum = binary.BigEndian.Uint32(row[pos : pos+4])
	pos += 4
	entry.HeaderVersion = binary.BigEndian.Uint16(row[pos : pos+2])
	if valueLength == 0 || valueLength > uint64(^uint64(0)>>1) || headerSize == 0 || headerSize > uint64(^uint64(0)>>1) || entry.HeaderVersion == 0 {
		return SegmentLiveIndexEntry{}, fmt.Errorf("invalid segment live index row facts")
	}
	entry.ValueLength = int64(valueLength)
	entry.HeaderSize = int64(headerSize)
	return entry, nil
}

// EncodeSegmentLiveIndexCoverage serializes the marker fingerprint.
func EncodeSegmentLiveIndexCoverage(coverage SegmentLiveIndexCoverage) ([]byte, error) {
	if coverage.DataBytes < 0 || coverage.Size < 0 {
		return nil, fmt.Errorf("invalid segment live index coverage")
	}
	value := make([]byte, 1+4+8+8)
	value[0] = SegmentLiveIndexVersion
	binary.BigEndian.PutUint32(value[1:5], coverage.Entries)
	binary.BigEndian.PutUint64(value[5:13], uint64(coverage.DataBytes))
	binary.BigEndian.PutUint64(value[13:21], uint64(coverage.Size))
	return value, nil
}

// DecodeSegmentLiveIndexCoverage decodes a marker fingerprint.
func DecodeSegmentLiveIndexCoverage(value []byte) (SegmentLiveIndexCoverage, error) {
	if len(value) != 1+4+8+8 || value[0] != SegmentLiveIndexVersion {
		return SegmentLiveIndexCoverage{}, fmt.Errorf("invalid segment live index coverage")
	}
	dataBytes := binary.BigEndian.Uint64(value[5:13])
	size := binary.BigEndian.Uint64(value[13:21])
	if dataBytes > uint64(^uint64(0)>>1) || size > uint64(^uint64(0)>>1) {
		return SegmentLiveIndexCoverage{}, fmt.Errorf("segment live index coverage is too large")
	}
	return SegmentLiveIndexCoverage{
		Entries:   binary.BigEndian.Uint32(value[1:5]),
		DataBytes: int64(dataBytes),
		Size:      int64(size),
	}, nil
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

// MakeFifoIndexKey creates a FIFO eviction index key that embeds the write time
// so entries sort oldest-written first.
// Format: !fifo/<write_time_nano>/<key>
func MakeFifoIndexKey(key string, writeTime time.Time) []byte {
	return fmt.Appendf(nil, "%s%019d/%s", FifoIndexPrefix, writeTime.UnixNano(), key)
}

// GetFifoIndexPrefix returns the prefix used to iterate FIFO index entries
// (oldest first, since the write-time nanos sort lexicographically).
func GetFifoIndexPrefix() []byte {
	return []byte(FifoIndexPrefix)
}

// MakeFifoBackrefKey creates the FIFO secondary-index key mapping a cache key to
// its current FIFO entry.
// Format: !fifo_ref/<key>
func MakeFifoBackrefKey(key string) []byte {
	return fmt.Appendf(nil, "%s%s", FifoBackrefPrefix, key)
}

// ParseFifoIndexKey extracts the original user key from a FIFO index key.
// Format: !fifo/<19-digit nano>/<key>
func ParseFifoIndexKey(fifoKey []byte) (string, error) {
	s := string(fifoKey)
	if len(s) < len(FifoIndexPrefix) || s[:len(FifoIndexPrefix)] != FifoIndexPrefix {
		return "", fmt.Errorf("invalid fifo index key: bad prefix")
	}
	rest := s[len(FifoIndexPrefix):] // "<19 digits>/<key>"
	// 19 nanos digits followed by '/'
	if len(rest) < 20 || rest[19] != '/' {
		return "", fmt.Errorf("invalid fifo index key: missing timestamp separator")
	}
	return rest[20:], nil
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
