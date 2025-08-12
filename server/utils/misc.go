package utils

import (
	"github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"

	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"google.golang.org/protobuf/proto"
)

// GetMetadata fetches metadata from the metaDB
func GetMetadata(meta *metadata.MetaDB, key string) (*pb.ValueMessage, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	metaSlice, err := meta.Handle().Get(ro, []byte(key))
	if err != nil || !metaSlice.Exists() {
		if metaSlice != nil {
			metaSlice.Free()
		}

		return nil, err
	}
	defer metaSlice.Free()

	var metadata pb.ValueMessage
	if err := proto.Unmarshal(metaSlice.Data(), &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// ValidateFileEntry checks if a file entry is valid by validating against provided metadata.
// Returns nil if the entry is valid, or a specific error if it's stale:
// - ErrMetadataNotFound: The metadata is nil (doesn't exist)
// - ErrAlreadyCompacted: The file has been compacted to a segment
// - ErrNotRawFile: The value is not a raw file (could be inline or other type)
// - ErrFilePathMismatch: The metadata points to a different file path
func ValidateFileEntry(metadata *pb.ValueMessage, filePath string, context string, key string) error {
	// Check if metadata exists
	if metadata == nil {
		// Metadata doesn't exist - entry is stale
		zlog.Debug().
			Str("key", key).
			Str("filepath", filePath).
			Msg(context + ": stale entry (metadata not found)")
		return ErrMetadataNotFound
	}

	// Check if metadata still points to a raw file
	if metadata.ValueType != pb.ValueType_RAW_FILE {
		// File was compacted or changed to inline
		if metadata.ValueType == pb.ValueType_SEGMENT {
			zlog.Debug().
				Str("key", key).
				Str("file", filePath).
				Msg(context + ": already compacted")
			return ErrAlreadyCompacted
		}
		zlog.Debug().
			Str("key", key).
			Str("filepath", filePath).
			Str("value_type", metadata.ValueType.String()).
			Msg(context + ": stale entry (not raw file)")
		return ErrNotRawFile
	}

	// Check if metadata still points to this specific file
	if metadata.RawFilePath != filePath {
		// Key was updated with new file
		zlog.Debug().
			Str("key", key).
			Str("old_file", filePath).
			Str("new_file", metadata.RawFilePath).
			Msg(context + ": stale entry (metadata updated)")
		return ErrFilePathMismatch
	}

	return nil
}
