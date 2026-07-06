package metadata

import (
	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
)

const (
	// Default block cache size in bytes
	DefaultRocksDBBlockCacheSize = 1 << 30 // 1GB

	// DefaultRocksDBMaxBackgroundJobs is the default number of concurrent
	// RocksDB background jobs (compactions + flushes) for the lifetime of the DB.
	DefaultRocksDBMaxBackgroundJobs = 8
)

// RocksDBConfig holds all RocksDB-specific configuration parameters
type RocksDBConfig struct {
	// Block cache size in bytes
	BlockCacheSize int64
	// Write buffer size in bytes
	WriteBufferSize int
	// Maximum number of write buffers
	MaxWriteBufferNumber int
	// Minimum write buffers to merge
	MinWriteBufferNumberToMerge int
	// Total write buffer size limit
	DbWriteBufferSize int64
	// Block size in bytes
	BlockSize int
	// Bloom filter bits per key
	BloomBitsPerKey int
	// Enable statistics collection
	EnableStatistics bool
	// Target file size base in bytes
	TargetFileSizeBase int64
	// Maximum background jobs
	MaxBackgroundJobs int
	// Level0 file number compaction trigger
	Level0FileNumCompactionTrigger int
	// Level0 slowdown writes trigger
	Level0SlowdownWritesTrigger int
	// Level0 stop writes trigger
	Level0StopWritesTrigger int
}

// DefaultRocksDBConfig returns the default configuration optimized for 4KB values with prefixes
func DefaultRocksDBConfig() *RocksDBConfig {
	return &RocksDBConfig{
		BlockCacheSize:                 DefaultRocksDBBlockCacheSize,
		WriteBufferSize:                64 * 1024 * 1024,  // 64MB
		MaxWriteBufferNumber:           6,                 // 6 buffers
		MinWriteBufferNumberToMerge:    2,                 // Merge 2 buffers
		DbWriteBufferSize:              384 * 1024 * 1024, // 384MB total
		BlockSize:                      16 * 1024,         // 16KB blocks
		BloomBitsPerKey:                12,                // 12 bits for bloom filter
		EnableStatistics:               true,              // Enable stats
		TargetFileSizeBase:             64 * 1024 * 1024,  // 64MB files
		MaxBackgroundJobs:              DefaultRocksDBMaxBackgroundJobs,
		Level0FileNumCompactionTrigger: 3,  // Trigger at 3 L0 files
		Level0SlowdownWritesTrigger:    10, // Slowdown at 10 L0 files
		Level0StopWritesTrigger:        20, // Stop at 20 L0 files
	}
}

// CreatePrefixExtractor creates a prefix extractor for the given prefix length
func CreatePrefixExtractor(prefixLength int) grocksdb.SliceTransform {
	if prefixLength <= 0 {
		return nil
	}
	return grocksdb.NewFixedPrefixTransform(prefixLength)
}

// CreateBlockBasedTableOptions creates optimized table options for 4KB values with prefixes
func CreateBlockBasedTableOptions(config *RocksDBConfig) *grocksdb.BlockBasedTableOptions {
	tableOpts := grocksdb.NewDefaultBlockBasedTableOptions()

	// Create and configure block cache
	if config.BlockCacheSize > 0 {
		blockCache := grocksdb.NewLRUCache(uint64(config.BlockCacheSize))
		tableOpts.SetBlockCache(blockCache)

		// Cache index and filter blocks with high priority
		tableOpts.SetCacheIndexAndFilterBlocks(true)
		tableOpts.SetPinL0FilterAndIndexBlocksInCache(true)

		// Pin top level index and filter for better performance
		tableOpts.SetPinTopLevelIndexAndFilter(true)
		tableOpts.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	}

	// Set block size optimized for 4KB values
	if config.BlockSize > 0 {
		tableOpts.SetBlockSize(config.BlockSize)
		tableOpts.SetBlockRestartInterval(4) // Restart every 4 entries for 4KB values
	}

	// Configure bloom filter for prefix optimization
	if config.BloomBitsPerKey > 0 {
		tableOpts.SetFilterPolicy(grocksdb.NewBloomFilter(float64(config.BloomBitsPerKey)))
		tableOpts.SetWholeKeyFiltering(false) // Use prefix filtering instead
		tableOpts.SetOptimizeFiltersForMemory(true)
	}

	// Set index type for efficient prefix lookups
	// Note: Index type and data block index type configuration may vary by grocksdb version
	// These are set to defaults that should work with most versions
	tableOpts.SetPartitionFilters(true)
	tableOpts.SetMetadataBlockSize(4096) // 4KB metadata blocks

	return tableOpts
}

// CreateOptions creates optimized RocksDB options for 4KB values with prefixes
func CreateOptions(config *RocksDBConfig, mergeOp grocksdb.MergeOperator) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Set block-based table options
	tableOpts := CreateBlockBasedTableOptions(config)
	opts.SetBlockBasedTableFactory(tableOpts)

	// Configure write buffers for 4KB values
	if config.WriteBufferSize > 0 {
		opts.SetWriteBufferSize(uint64(config.WriteBufferSize))
	}
	if config.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(config.MaxWriteBufferNumber)
	}
	if config.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(config.MinWriteBufferNumberToMerge)
	}
	if config.DbWriteBufferSize > 0 {
		opts.SetDbWriteBufferSize(uint64(config.DbWriteBufferSize))
	}

	// Set arena block size for memory efficiency
	opts.SetArenaBlockSize(2 * 1024 * 1024) // 2MB arena blocks for 4KB values

	// Configure memtable for prefix optimization
	// Using hash skip list for better performance with prefixes
	// opts.SetHashSkipListRep(100000, 8, 3)
	// opts.SetMemTablePrefixBloomSizeRatio(0.15) // 15% of memtable for prefix bloom

	// Compaction configuration
	if config.Level0FileNumCompactionTrigger > 0 {
		opts.SetLevel0FileNumCompactionTrigger(config.Level0FileNumCompactionTrigger)
	}
	if config.Level0SlowdownWritesTrigger > 0 {
		opts.SetLevel0SlowdownWritesTrigger(config.Level0SlowdownWritesTrigger)
	}
	if config.Level0StopWritesTrigger > 0 {
		opts.SetLevel0StopWritesTrigger(config.Level0StopWritesTrigger)
	}

	// File sizes optimized for 4KB values
	if config.TargetFileSizeBase > 0 {
		opts.SetTargetFileSizeBase(uint64(config.TargetFileSizeBase))
		opts.SetTargetFileSizeMultiplier(2) // 2x multiplier for higher levels
	}

	// Level sizes
	opts.SetMaxBytesForLevelBase(192 * 1024 * 1024) // 192MB for L1
	opts.SetMaxBytesForLevelMultiplier(10)          // 10x multiplier

	// Background operations
	if config.MaxBackgroundJobs > 0 {
		opts.SetMaxBackgroundJobs(config.MaxBackgroundJobs)
	}

	// Performance optimizations
	opts.SetOptimizeFiltersForHits(true)
	opts.SetLevelCompactionDynamicLevelBytes(true)
	opts.SetAdviseRandomOnOpen(true)

	// Concurrent write optimizations
	opts.SetAllowConcurrentMemtableWrites(true)
	opts.SetEnableWriteThreadAdaptiveYield(true)
	opts.SetEnablePipelinedWrite(true)

	// WAL optimizations
	opts.SetWALRecoveryMode(grocksdb.PointInTimeRecovery)
	opts.SetWALBytesPerSync(512 * 1024) // Sync every 512KB
	opts.SetBytesPerSync(512 * 1024)    // Sync SST files every 512KB

	// Enable statistics if configured
	if config.EnableStatistics {
		opts.SetStatsDumpPeriodSec(600) // Dump stats every 10 minutes
	}

	// Set merge operator if provided
	if mergeOp != nil {
		opts.SetMergeOperator(mergeOp)
	}

	return opts
}

// LogConfiguration logs the RocksDB configuration for debugging
func LogConfiguration(config *RocksDBConfig) {
	zlog.Info().
		Int64("block_cache_size_gb", config.BlockCacheSize>>30).
		Int("write_buffer_size_mb", config.WriteBufferSize>>20).
		Int("max_write_buffers", config.MaxWriteBufferNumber).
		Int64("db_write_buffer_size_mb", config.DbWriteBufferSize>>20).
		Int("block_size_kb", config.BlockSize>>10).
		Int("bloom_bits_per_key", config.BloomBitsPerKey).
		Int64("target_file_size_mb", config.TargetFileSizeBase>>20).
		Int("max_background_jobs", config.MaxBackgroundJobs).
		Int("l0_compaction_trigger", config.Level0FileNumCompactionTrigger).
		Msg("RocksDB configuration initialized")
}

// CreateReadOptions creates optimized read options for prefix-based queries
func CreateReadOptions(prefixSeek bool, cacheBlocks bool) *grocksdb.ReadOptions {
	ro := grocksdb.NewDefaultReadOptions()

	if prefixSeek {
		// Optimize for prefix iteration
		ro.SetPrefixSameAsStart(true)
		ro.SetTotalOrderSeek(false) // Use prefix ordering
	}

	ro.SetFillCache(cacheBlocks) // Cache reads based on cacheBlocks parameter

	return ro
}
