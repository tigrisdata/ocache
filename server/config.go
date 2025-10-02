package main

import "time"

// Config holds all configuration options for the cache service. It's populated
// once at start-up via LoadConfig() and accessed via the global variable
// `AppConfig` from the rest of the codebase.
//

type Config struct {
	DiskPath               string        // Directory for on-disk cache data
	InlineThreshold        int           // Threshold for small objects that are inlined in RocksDB (bytes)
	CompactThreshold       int64         // Objects less than this size are compacted to segments (bytes)
	SegmentSize            int64         // Segment size (bytes)
	TTL                    int           // Default TTL when no key-level TTL is set (seconds)
	ListenAddr             string        // gRPC listen address
	ListenHTTP             string        // HTTP (grpc-gateway) listen address
	Verbose                bool          // Enable verbose/debug logging
	FdCacheSize            int           // Size of the file descriptor cache
	MaxDiskUsage           int64         // Maximum disk usage in bytes (0 = unlimited)
	CompactionThreads      int           // Number of compaction threads
	FragThreshold          float64       // Fragmentation threshold for segment recompaction (0.0-1.0)
	RecompactMinSegmentAge time.Duration // Minimum age for segment recompaction
	RecompactMinSegments   int           // Minimum number of segments for recompaction
	RecompactDisable       bool          // Disable automatic segment recompaction
	TTLCleanupInterval     time.Duration // TTL cleanup interval
	RequestLogging         bool          // Enable request logging
	// RocksDB tuning parameters
	MetadataCacheSize int64 // RocksDB block cache size in bytes

	// Cluster configuration
	ClusterEnabled    bool          // Enable cluster mode
	NodeID            string        // Unique node identifier in cluster
	ClusterAddr       string        // Address for cluster communication
	Seeds             []string      // Seed nodes for joining cluster
	PartitionCount    int           // Number of partitions in hash ring
	HeartbeatInterval time.Duration // Interval between heartbeats
	FailureThreshold  int           // Number of failed heartbeats before marking node down
}

// AppConfig is the singleton that stores the parsed configuration.
var AppConfig Config

// LoadConfig must be invoked after flag.Parse() so that the package-level
// flag variables in main.go are initialised. It copies the values from those
// flag variables into the global AppConfig variable.
func LoadConfig() {
	AppConfig = Config{
		DiskPath:               *diskPath,
		InlineThreshold:        *inlineThreshold,
		CompactThreshold:       *compactThreshold,
		SegmentSize:            *segmentSize,
		TTL:                    *ttl,
		ListenAddr:             *listenAddr,
		ListenHTTP:             *listenHTTP,
		Verbose:                *verbose,
		FdCacheSize:            *fdCacheSize,
		MaxDiskUsage:           *maxDiskUsage,
		CompactionThreads:      *compactionThreads,
		FragThreshold:          *fragThreshold,
		RecompactMinSegmentAge: *recompactMinSegmentAge,
		RecompactMinSegments:   *recompactMinSegments,
		RecompactDisable:       *recompactDisable,
		TTLCleanupInterval:     *ttlCleanupInterval,
		RequestLogging:         *requestLogging,
		MetadataCacheSize:      *metadataCacheSize,
		ClusterEnabled:         *clusterEnabled,
		NodeID:                 *nodeID,
		ClusterAddr:            *clusterAddr,
		Seeds:                  seeds,
		PartitionCount:         *partitionCount,
		HeartbeatInterval:      *heartbeatInterval,
		FailureThreshold:       *failureThreshold,
	}
}
