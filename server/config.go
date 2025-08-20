package main

import "time"

// Config holds all configuration options for the cache service. It's populated
// once at start-up via LoadConfig() and accessed via the global variable
// `AppConfig` from the rest of the codebase.
//

type Config struct {
	DiskPath           string        // Directory for on-disk cache data
	InlineThreshold    int           // Threshold for small objects that are inlined in RocksDB (bytes)
	CompactThreshold   int64         // Objects less than this size are compacted to segments (bytes)
	SegmentSize        int64         // Segment size (bytes)
	TTL                int           // Default TTL when no key-level TTL is set (seconds)
	Port               int           // gRPC listen port
	HTTPPort           int           // HTTP (grpc-gateway) port
	Verbose            bool          // Enable verbose/debug logging
	FdCacheSize        int           // Size of the file descriptor cache
	MaxDiskUsage       int64         // Maximum disk usage in bytes (0 = unlimited)
	CompactionInterval time.Duration // Compaction interval
	FragThreshold      float64       // Fragmentation threshold for segment recompaction (0.0-1.0)
	RecompactDisable   bool          // Disable automatic segment recompaction
}

// AppConfig is the singleton that stores the parsed configuration.
var AppConfig Config

// LoadConfig must be invoked after flag.Parse() so that the package-level
// flag variables in main.go are initialised. It copies the values from those
// flag variables into the global AppConfig variable.
func LoadConfig() {
	AppConfig = Config{
		DiskPath:           *diskPath,
		InlineThreshold:    *inlineThreshold,
		CompactThreshold:   *compactThreshold,
		SegmentSize:        *segmentSize,
		TTL:                *ttl,
		Port:               *port,
		HTTPPort:           *httpPort,
		Verbose:            *verbose,
		FdCacheSize:        *fdCacheSize,
		MaxDiskUsage:       *maxDiskUsage,
		CompactionInterval: *compactionInterval,
		FragThreshold:      *fragThreshold,
		RecompactDisable:   *recompactDisable,
	}
}
