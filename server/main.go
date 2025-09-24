package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/hash"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	stor "github.com/tigrisdata/ocache/storage"
)

var (
	diskPath = flag.String("disk", stor.DefaultDiskPath, "Directory for disk cache")
	ttl      = flag.Int("ttl", stor.DefaultTTL, "Default global TTL in seconds when no key-level TTL is set")

	inlineThreshold        = flag.Int("threshold", stor.DefaultInlineThreshold, "Small object threshold (bytes) that are inlined with metadata")
	compactThreshold       = flag.Int64("compact-threshold", stor.DefaultCompactThreshold, "Compaction threshold (bytes)")
	segmentSize            = flag.Int64("segment-size", stor.DefaultSegmentSize, "Segment size (bytes)")
	compactionInterval     = flag.Duration("compaction-interval", stor.DefaultCompactionInterval, "Compaction interval")
	compactionThreads      = flag.Int("compaction-threads", stor.DefaultCompactionThreads, "Number of compaction threads")
	fragThreshold          = flag.Float64("fragmentation-threshold", stor.DefaultFragmentationThreshold, "Segment fragmentation threshold for recompaction (0.0-1.0)")
	recompactMinSegmentAge = flag.Duration("recompaction-min-segment-age", stor.DefaultMinSegmentAgeForRecompaction, "Minimum age for segment recompaction")
	recompactMinSegments   = flag.Int("recompaction-min-segments", stor.DefaultMinSegmentsBeforeRecompaction, "Minimum number of segments for recompaction")
	recompactDisable       = flag.Bool("disable-recompaction", stor.DefaultRecompactionDisabled, "Disable automatic segment recompaction")
	ttlCleanupInterval     = flag.Duration("ttl-cleanup-interval", stor.DefaultTTLCleanupInterval, "Interval at which TTL keys are cleaned up")

	maxDiskUsage = flag.Int64("max-disk-usage", stor.DefaultMaxDiskUsage, "Maximum disk usage in bytes (0 = unlimited, uses LRU eviction)")
	fdCacheSize  = flag.Int("fd-cache-size", stor.DefaultFdCacheSize, "Size of the file descriptor cache (entries)")

	metadataCacheSize = flag.Int64("metadata-cache-size", stor.DefaultMetadataCacheSize, "Metadata cache size in bytes (default: 1GB)")

	listenAddr     = flag.String("listen-addr", ":9000", "Listen address for gRPC server")
	listenHTTP     = flag.String("listen-http", ":9001", "Listen address for HTTP/grpc-gateway server")
	verbose        = flag.Bool("v", false, "Enable debug logging")
	requestLogging = flag.Bool("request-logging", false, "Enable request logging")

	// Cluster configuration flags
	clusterEnabled    = flag.Bool("cluster-enabled", false, "Enable cluster mode")
	nodeID            = flag.String("node-id", "", "Unique node identifier (required in cluster mode)")
	clusterAddr       = flag.String("cluster-addr", ":7000", "Address for cluster communication")
	seedsStr          = flag.String("seeds", "", "Comma-separated list of seed nodes (e.g., node1:7000,node2:7000 or ocache.svc.cluster.local:7000)")
	partitionCount    = flag.Int("partition-count", hash.DefaultPartitionCount, "Number of partitions in hash ring")
	heartbeatInterval = flag.Duration("heartbeat-interval", coordinator.DefaultHeartbeatInterval, "Interval between heartbeats")
	failureThreshold  = flag.Int("failure-threshold", coordinator.DefaultFailureThreshold, "Number of failed heartbeats before marking node down")

	seeds []string

	// Global coordinator instance
	globalCoordinator *coordinator.Coordinator
)

func configureLogger() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if AppConfig.Verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)

		// Also enable request logging
		AppConfig.RequestLogging = true
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// initializeCluster sets up the cluster coordinator if clustering is enabled
func initializeCluster(ctx context.Context) {
	if !AppConfig.ClusterEnabled {
		return
	}

	coordConfig := &coordinator.Config{
		Enabled:            true,
		MyNodeID:           AppConfig.NodeID,
		ClusterAddr:        AppConfig.ClusterAddr,
		Nodes:              AppConfig.Seeds,
		RingPartitionCount: AppConfig.PartitionCount,
		HeartbeatInterval:  int(AppConfig.HeartbeatInterval.Seconds()),
		FailureThreshold:   AppConfig.FailureThreshold,
	}

	var err error
	globalCoordinator, err = coordinator.New(coordConfig)
	if err != nil {
		zlog.Fatal().Err(err).Msg("Failed to create coordinator")
	}

	if err := globalCoordinator.Start(ctx); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to start coordinator")
	}

	zlog.Info().
		Str("node_id", AppConfig.NodeID).
		Str("cluster_addr", AppConfig.ClusterAddr).
		Int("seeds", len(AppConfig.Seeds)).
		Msg("Cluster coordinator started")
}

// initializeStorage sets up the storage layer
func initializeStorage() {
	storageConfig := &stor.StorageConfig{
		DiskPath:            AppConfig.DiskPath,
		TTL:                 AppConfig.TTL,
		InlineThreshold:     AppConfig.InlineThreshold,
		CompactThreshold:    AppConfig.CompactThreshold,
		SegmentSize:         AppConfig.SegmentSize,
		FdCacheSize:         AppConfig.FdCacheSize,
		MaxDiskUsage:        AppConfig.MaxDiskUsage,
		CompactionInterval:  AppConfig.CompactionInterval,
		CompactionThreads:   AppConfig.CompactionThreads,
		FragThreshold:       AppConfig.FragThreshold,
		MinSegmentAge:       AppConfig.RecompactMinSegmentAge,
		MinSegments:         AppConfig.RecompactMinSegments,
		DisableRecompaction: AppConfig.RecompactDisable,
		CleanupInterval:     AppConfig.TTLCleanupInterval,
		MetadataCacheSize:   AppConfig.MetadataCacheSize,
	}
	stor.InitStorageWithConfig(storageConfig)
}

// startUserServices starts the user-facing gRPC and HTTP gateway services
func startUserServices() {
	go startGRPCServer()                                // Start gRPC server in goroutine
	go startGRPCGatewayServer(*listenAddr, *listenHTTP) // Start grpc-gateway on different address
}

// waitForShutdown waits for shutdown signal or coordinator error
func waitForShutdown(ctx context.Context, cancel context.CancelFunc) {
	// Handle graceful shutdown on SIGINT/SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create coordinator error channel if coordinator is enabled
	var coordinatorErrCh <-chan error
	if globalCoordinator != nil {
		coordinatorErrCh = globalCoordinator.ErrorChan()
	}

	// Wait for shutdown signal or fatal coordinator error
	select {
	case sig := <-sigChan:
		zlog.Info().Str("signal", sig.String()).Msg("Received shutdown signal, shutting down...")
	case err := <-coordinatorErrCh:
		if err != nil {
			zlog.Error().Err(err).Msg("Coordinator reported fatal error, shutting down...")
		}
	}

	// Cancel context to signal graceful shutdown
	cancel()
}

// performShutdown handles graceful shutdown of all components
func performShutdown() {
	// Close coordinator if enabled
	if globalCoordinator != nil {
		if err := globalCoordinator.Stop(); err != nil {
			zlog.Error().Err(err).Msg("Error stopping coordinator")
		}
	}

	// Close storage (flush segments, close RocksDB, etc.)
	stor.CloseStorage()

	zlog.Info().Msg("Shutdown complete")
}

func RunServer() {
	// Create a context for the server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Prometheus metrics
	metrics.Init()
	zlog.Info().Msg("Prometheus metrics initialized")

	// Set up internal cluster communication if enabled
	initializeCluster(ctx)

	// Initialize the storage layer
	initializeStorage()

	// Start gRPC and HTTP gateway for client requests
	startUserServices()

	// Wait for shutdown signal
	waitForShutdown(ctx, cancel)

	// Perform graceful shutdown
	performShutdown()
}

func main() {
	flag.Parse()

	// Parse seed nodes if provided
	if *seedsStr != "" {
		seeds = strings.Split(*seedsStr, ",")
		for i, seed := range seeds {
			seeds[i] = strings.TrimSpace(seed)
		}
	}

	LoadConfig()
	configureLogger()

	RunServer() // Initialize and run the server
}
