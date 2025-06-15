package metadata

import (
	grocksdb "github.com/linxGnu/grocksdb"
)

var metaDB *grocksdb.DB

// InitMetaDB initializes the global metadata DB. It should be called exactly
// once during Storage initialization.
func InitMetaDB(diskPath string, ttl int) (*grocksdb.DB, error) {
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

	// Cache the instance globally so future callers (e.g., RawFileManager) get a
	// valid handle instead of nil and won't crash when they attempt to use it.
	metaDB = db

	return db, nil
}

// GetMetaDB returns the global RocksDB instance used for metadata operations.
func GetMetaDB() *grocksdb.DB { return metaDB }
