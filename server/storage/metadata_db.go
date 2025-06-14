package storage

import (
	grocksdb "github.com/linxGnu/grocksdb"
)

var metaDB *grocksdb.DB

// initMetaDB initializes the global metadata DB. It should be called exactly
// once during Storage initialization.
func initMetaDB(diskPath string, ttl int) (*grocksdb.DB, error) {
	if metaDB != nil {
		return metaDB, nil
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	dbPath := diskPath + "/rocksdb"
	db, err := grocksdb.OpenDbWithTTL(opts, dbPath, ttl)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// getMetaDB returns the global RocksDB instance used for metadata operations.
func getMetaDB() *grocksdb.DB { return metaDB }
