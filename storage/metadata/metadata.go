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

// Close closes this MetaDB instance.
// This is safe to call on isolated instances.
func (m *MetaDB) Close() {
	if m != nil && m.handle != nil {
		m.handle.Close()
	}
}

var metaDB *MetaDB

// NewMetaDBWithConfig initializes the global metadata DB with optional custom configuration.
// It should be called exactly once during Storage initialization.
func NewMetaDBWithConfig(diskPath string, ttl int, mergeOp grocksdb.MergeOperator, config *RocksDBConfig) (*MetaDB, error) {
	if metaDB != nil {
		return metaDB, nil
	}

	zlog.Info().Str("diskPath", diskPath).Int("ttl", ttl).Msg("creating metadata DB with custom configuration")

	if config == nil {
		config = DefaultRocksDBConfig()
	}

	LogConfiguration(config)
	opts := CreateOptions(config, mergeOp)

	dbPath := diskPath + "/rocksdb"
	db, err := grocksdb.OpenDbWithTTL(opts, dbPath, ttl)
	if err != nil {
		return nil, err
	}

	metaDB = &MetaDB{handle: db}

	zlog.Info().Msg("metadata DB created with custom configuration")

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

	metaDB.Close()

	zlog.Info().Msg("metadata DB closed")

	metaDB = nil
}
