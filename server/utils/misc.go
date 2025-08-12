package utils

import (
	"github.com/linxGnu/grocksdb"

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
