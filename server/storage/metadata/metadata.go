package metadata

import (
	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
)

type MetaDB struct {
	handle *grocksdb.DB
}

// Handle returns the underlying RocksDB handle.
func (m *MetaDB) Handle() *grocksdb.DB {
	return m.handle
}

var metaDB *MetaDB

// NewMetaDB initializes the global metadata DB. It should be called exactly
// once during Storage initialization.
func NewMetaDB(diskPath string, ttl int, mergeOp grocksdb.MergeOperator) (*MetaDB, error) {
	if metaDB != nil {
		return metaDB, nil
	}

	zlog.Info().Str("diskPath", diskPath).Int("ttl", ttl).Msg("creating metadata DB")

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	
	// Set the merge operator if provided
	if mergeOp != nil {
		opts.SetMergeOperator(mergeOp)
	}

	dbPath := diskPath + "/rocksdb"
	db, err := grocksdb.OpenDbWithTTL(opts, dbPath, ttl)
	if err != nil {
		return nil, err
	}

	metaDB = &MetaDB{handle: db}

	zlog.Info().Msg("metadata DB created")

	return metaDB, nil
}

// GetMetaDB returns the global RocksDB instance used for metadata operations.
func GetMetaDB() *MetaDB { return metaDB }

// CloseMetaDB closes the global RocksDB instance used for metadata operations.
func CloseMetaDB() {
	if metaDB == nil {
		return
	}
	zlog.Info().Msg("closing metadata DB")

	metaDB.handle.Close()

	zlog.Info().Msg("metadata DB closed")

	metaDB = nil
}
