// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"time"
)

// TestObject represents a test object with metadata
type TestObject struct {
	Key      string
	Data     []byte
	Size     int64
	Checksum uint32
	TTL      *time.Duration
}

// GenerateSmallObjects generates small objects (1B - 64KB)
func GenerateSmallObjects(count int) []TestObject {
	objects := make([]TestObject, count)
	sizes := []int64{
		1,     // 1 byte
		100,   // 100 bytes
		1024,  // 1KB
		10240, // 10KB
		32768, // 32KB
		65535, // ~64KB (just under threshold)
		65536, // Exactly 64KB (boundary)
	}

	for i := 0; i < count; i++ {
		// Pick a size (cycle through predefined sizes, then random)
		var size int64
		if i < len(sizes) {
			size = sizes[i]
		} else {
			// Random size between 1B and 64KB
			size = int64(1 + (i*1000)%65536)
		}

		obj := generateTestObject(fmt.Sprintf("small-%d", i), size)
		objects[i] = obj
	}

	return objects
}

// GenerateMediumObjects generates medium objects (64KB - 16MB)
func GenerateMediumObjects(count int) []TestObject {
	objects := make([]TestObject, count)
	sizes := []int64{
		65537,            // Just over 64KB
		100 * 1024,       // 100KB
		512 * 1024,       // 512KB
		1024 * 1024,      // 1MB
		5 * 1024 * 1024,  // 5MB
		10 * 1024 * 1024, // 10MB
		16*1024*1024 - 1, // Just under 16MB
		64 * 1024 * 1024, // Exactly 64MB (boundary)
	}

	for i := 0; i < count; i++ {
		// Pick a size (cycle through predefined sizes, then random)
		var size int64
		if i < len(sizes) {
			size = sizes[i]
		} else {
			// Random size between 64KB and 64MB
			minSize := int64(64*1024 + 1)
			maxSize := int64(64 * 1024 * 1024)
			size = minSize + int64(i*100000)%(maxSize-minSize)
		}

		obj := generateTestObject(fmt.Sprintf("medium-%d", i), size)
		objects[i] = obj
	}

	return objects
}

// GenerateLargeObjects generates large objects (16MB - 256MB)
func GenerateLargeObjects(count int) []TestObject {
	objects := make([]TestObject, count)
	sizes := []int64{
		16*1024*1024 + 1,  // Just over 16MB
		20 * 1024 * 1024,  // 20MB
		50 * 1024 * 1024,  // 50MB
		100 * 1024 * 1024, // 100MB
		200 * 1024 * 1024, // 200MB
		256 * 1024 * 1024, // 256MB (segment size boundary)
	}

	for i := 0; i < count; i++ {
		// Pick a size (cycle through predefined sizes, then random)
		var size int64
		if i < len(sizes) {
			size = sizes[i]
		} else {
			// Random size between 16MB and 256MB
			minSize := int64(16*1024*1024 + 1)
			maxSize := int64(256 * 1024 * 1024)
			size = minSize + int64(i*1000000)%(maxSize-minSize)
		}

		obj := generateTestObject(fmt.Sprintf("large-%d", i), size)
		objects[i] = obj
	}

	return objects
}

// GenerateMixedObjects generates a mix of small, medium, and large objects
func GenerateMixedObjects(small, medium, large int) []TestObject {
	objects := make([]TestObject, 0, small+medium+large)

	// Generate and append each type
	objects = append(objects, GenerateSmallObjects(small)...)
	objects = append(objects, GenerateMediumObjects(medium)...)
	objects = append(objects, GenerateLargeObjects(large)...)

	return objects
}

// generateTestObject creates a single test object with the specified size
func generateTestObject(key string, size int64) TestObject {
	data := GenerateRandomData(size)
	checksum := crc32.ChecksumIEEE(data)

	return TestObject{
		Key:      key,
		Data:     data,
		Size:     size,
		Checksum: checksum,
		TTL:      nil,
	}
}

// GenerateSequentialData generates sequential bytes of specified size (more memory efficient for large data)
func GenerateSequentialData(size int64) []byte {
	data := make([]byte, size)
	for i := int64(0); i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

// GenerateRandomData generates random data of specified size
func GenerateRandomData(size int64) []byte {
	data := make([]byte, size)

	if size <= 1024*1024 { // For sizes up to 1MB, use crypto/rand
		rand.Read(data)
	} else {
		// For larger sizes, use a faster method with repeated patterns
		// This is more efficient for large test data
		pattern := make([]byte, 1024)
		rand.Read(pattern)

		for i := int64(0); i < size; i += 1024 {
			end := i + 1024
			if end > size {
				end = size
			}
			copy(data[i:end], pattern[:end-i])
		}

		// Add some randomness to avoid too much repetition
		for i := int64(0); i < size; i += 10240 {
			if i < size {
				data[i] = byte(i % 256)
			}
		}
	}

	return data
}

// GeneratePatternData generates data with a repeating pattern (useful for compression tests)
func GeneratePatternData(size int64, pattern string) []byte {
	patternBytes := []byte(pattern)
	data := make([]byte, size)

	for i := int64(0); i < size; i++ {
		data[i] = patternBytes[i%int64(len(patternBytes))]
	}

	return data
}

// GenerateCompressibleData generates highly compressible data
func GenerateCompressibleData(size int64) []byte {
	// Use a repeating pattern that compresses well
	return GeneratePatternData(size, "AAABBBCCCDDDEEEFFFGGGHHHIIIJJJKKKLLLMMMNNNOOOPPPQQQRRRSSSTTTUUUVVVWWWXXXYYYZZZ")
}

// GenerateBinaryData generates binary data with specific byte patterns
func GenerateBinaryData(size int64) []byte {
	data := make([]byte, size)

	for i := int64(0); i < size; i++ {
		// Create a pattern of binary data including null bytes
		switch i % 4 {
		case 0:
			data[i] = 0x00 // Null byte
		case 1:
			data[i] = 0xFF // All ones
		case 2:
			data[i] = 0xAA // Alternating pattern
		case 3:
			data[i] = byte(i % 256) // Sequential
		}
	}

	return data
}

// GenerateUnicodeData generates data with Unicode characters
func GenerateUnicodeData(size int64) []byte {
	// Create a string with various Unicode characters
	unicodeChars := "Hello 世界 🌍 Здравствуй мир مرحبا بالعالم"
	pattern := []byte(unicodeChars)

	data := make([]byte, 0, size)
	for int64(len(data)) < size {
		data = append(data, pattern...)
	}

	// Trim to exact size
	if int64(len(data)) > size {
		data = data[:size]
	}

	return data
}

// GenerateObjectsWithTTL generates objects with specific TTL values
func GenerateObjectsWithTTL(count int, size int64, ttl time.Duration) []TestObject {
	objects := make([]TestObject, count)

	for i := 0; i < count; i++ {
		obj := generateTestObject(fmt.Sprintf("ttl-%d", i), size)
		obj.TTL = &ttl
		objects[i] = obj
	}

	return objects
}

// GenerateSequentialKeys generates objects with sequential keys
func GenerateSequentialKeys(prefix string, start, end int, size int64) []TestObject {
	count := end - start + 1
	objects := make([]TestObject, count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, start+i)
		objects[i] = generateTestObject(key, size)
	}

	return objects
}

// GenerateEdgeCaseObjects generates objects for edge case testing
func GenerateEdgeCaseObjects() []TestObject {
	objects := []TestObject{
		// Empty object
		generateTestObject("empty", 0),

		// Single byte
		generateTestObject("single-byte", 1),

		// Exactly at thresholds
		generateTestObject("exactly-64kb", 64*1024),
		generateTestObject("exactly-16mb", 16*1024*1024),
		generateTestObject("exactly-256mb", 256*1024*1024),

		// Just below thresholds
		generateTestObject("just-below-64kb", 64*1024-1),
		generateTestObject("just-below-16mb", 16*1024*1024-1),
		generateTestObject("just-below-256mb", 256*1024*1024-1),

		// Just above thresholds
		generateTestObject("just-above-64kb", 64*1024+1),
		generateTestObject("just-above-16mb", 16*1024*1024+1),
	}

	// Add objects with special data patterns
	specialData := []struct {
		key  string
		data []byte
	}{
		{"all-zeros", bytes.Repeat([]byte{0}, 1024)},
		{"all-ones", bytes.Repeat([]byte{0xFF}, 1024)},
		{"binary-data", GenerateBinaryData(1024)},
		{"unicode-data", GenerateUnicodeData(1024)},
		{"compressible", GenerateCompressibleData(10240)},
	}

	for _, sd := range specialData {
		obj := TestObject{
			Key:      sd.key,
			Data:     sd.data,
			Size:     int64(len(sd.data)),
			Checksum: crc32.ChecksumIEEE(sd.data),
			TTL:      nil,
		}
		objects = append(objects, obj)
	}

	return objects
}

// CalculateChecksum calculates the CRC32 checksum of data
func CalculateChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ValidateChecksum validates the checksum of data
func ValidateChecksum(data []byte, expectedChecksum uint32) bool {
	actualChecksum := crc32.ChecksumIEEE(data)
	return actualChecksum == expectedChecksum
}
