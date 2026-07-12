// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

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

// NewMetaDB creates a new isolated MetaDB instance with custom configuration.
// The caller is responsible for calling Close() on the returned instance.
func NewMetaDB(diskPath string, ttl int, mergeOp grocksdb.MergeOperator, config *RocksDBConfig) (*MetaDB, error) {
	zlog.Info().Str("diskPath", diskPath).Int("ttl", ttl).Msg("creating isolated metadata DB instance with custom configuration")

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

	instance := &MetaDB{handle: db}

	zlog.Info().Msg("isolated metadata DB instance created with custom configuration")

	return instance, nil
}
