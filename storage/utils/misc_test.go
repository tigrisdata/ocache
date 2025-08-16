package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/tigrisdata/ocache/proto"
)

func TestValidateFileEntry(t *testing.T) {
	// Test case 1: Metadata doesn't exist (nil) - should return ErrMetadataNotFound
	t.Run("metadata_nil", func(t *testing.T) {
		err := ValidateFileEntry(nil, "/path/to/file", "test", "key1")
		assert.True(t, errors.Is(err, ErrMetadataNotFound), "Should return ErrMetadataNotFound when metadata is nil")
	})

	// Test case 2: Metadata exists with matching file path - should return nil (valid)
	t.Run("matching_file_path", func(t *testing.T) {
		filePath := "/path/to/test.dat"

		// Create metadata pointing to the file
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: filePath,
			ValueLength: 1024,
		}

		// Validate - should be nil (valid)
		err := ValidateFileEntry(vm, filePath, "test", "key2")
		assert.NoError(t, err, "Entry should be valid when file path matches")
	})

	// Test case 3: Metadata points to different file - should return ErrFilePathMismatch
	t.Run("different_file_path", func(t *testing.T) {
		oldPath := "/path/to/old.dat"
		newPath := "/path/to/new.dat"

		// Create metadata pointing to new file
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: newPath,
			ValueLength: 1024,
		}

		// Validate old path - should return ErrFilePathMismatch
		err := ValidateFileEntry(vm, oldPath, "test", "key3")
		assert.True(t, errors.Is(err, ErrFilePathMismatch), "Should return ErrFilePathMismatch when paths don't match")
	})

	// Test case 4: Metadata is SEGMENT (compacted) - should return ErrAlreadyCompacted
	t.Run("already_compacted", func(t *testing.T) {
		filePath := "/path/to/compacted.dat"

		// Create metadata for compacted entry
		vm := &pb.ValueMessage{
			ValueType:     pb.ValueType_SEGMENT,
			SegmentPath:   "/path/to/segment",
			SegmentOffset: 12345,
			ValueLength:   1024,
		}

		// Validate - should return ErrAlreadyCompacted
		err := ValidateFileEntry(vm, filePath, "test", "key4")
		assert.True(t, errors.Is(err, ErrAlreadyCompacted), "Should return ErrAlreadyCompacted for SEGMENT type")
	})

	// Test case 5: Metadata is INLINE - should return ErrNotRawFile
	t.Run("inline_value", func(t *testing.T) {
		filePath := "/path/to/inline.dat"

		// Create metadata for inline entry
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_INLINE,
			ValueLength: 100,
		}

		// Validate - should return ErrNotRawFile
		err := ValidateFileEntry(vm, filePath, "test", "key5")
		assert.True(t, errors.Is(err, ErrNotRawFile), "Should return ErrNotRawFile for INLINE type")
	})
}
