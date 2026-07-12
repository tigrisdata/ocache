// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/server/service"
	stor "github.com/tigrisdata/ocache/storage"
)

var (
	diskPath = flag.String("disk", stor.DefaultDiskPath, "Directory for disk cache")
	ttl      = flag.Int("ttl", stor.DefaultTTL, "Default global TTL in seconds when no key-level TTL is set")

	inlineThreshold        = flag.Int("threshold", stor.DefaultInlineThreshold, "Small object threshold (bytes) that are inlined with metadata")
	compactThreshold       = flag.Int64("compact-threshold", stor.DefaultCompactThreshold, "Compaction threshold (bytes)")
	segmentSize            = flag.Int64("segment-size", stor.DefaultSegmentSize, "Segment size (bytes)")
	compactionThreads      = flag.Int("compaction-threads", stor.DefaultCompactionThreads, "Number of compaction threads")
	fragThreshold          = flag.Float64("fragmentation-threshold", stor.DefaultFragmentationThreshold, "Segment fragmentation threshold for recompaction (0.0-1.0)")
	recompactMinSegmentAge = flag.Duration("recompaction-min-segment-age", stor.DefaultMinSegmentAgeForRecompaction, "Minimum age for segment recompaction")
	recompactMinSegments   = flag.Int("recompaction-min-segments", stor.DefaultMinSegmentsBeforeRecompaction, "Minimum number of segments for recompaction")
	recompactDisable       = flag.Bool("disable-recompaction", stor.DefaultRecompactionDisabled, "Disable automatic segment recompaction")
	ttlCleanupInterval     = flag.Duration("ttl-cleanup-interval", stor.DefaultTTLCleanupInterval, "Interval at which TTL keys are cleaned up")

	maxDiskUsage = flag.Int64("max-disk-usage", stor.DefaultMaxDiskUsage, "Maximum disk usage in bytes (0 = unlimited, uses LRU eviction)")
	fdCacheSize  = flag.Int("fd-cache-size", stor.DefaultFdCacheSize, "Size of the file descriptor cache (entries)")

	recoveryWorkers = flag.Int("recovery-workers", stor.DefaultRecoveryWorkers, "Number of parallel workers for startup file recovery")
	deleteBatchSize = flag.Int("delete-batch-size", stor.DefaultDeleteBatchSize, "Number of file deletions processed per deletion-queue batch")

	metadataCacheSize      = flag.Int64("metadata-cache-size", stor.DefaultMetadataCacheSize, "Metadata cache size in bytes (default: 1GB)")
	metadataBackgroundJobs = flag.Int("metadata-background-jobs", stor.DefaultMetadataBackgroundJobs, "Max concurrent RocksDB background jobs (compactions + flushes) over the process lifetime; caps background CPU without a container CPU limit")

	listenAddr     = flag.String("listen-addr", ":9000", "Listen address for gRPC server")
	listenHTTP     = flag.String("listen-http", ":9001", "Listen address for HTTP/grpc-gateway server")
	verbose        = flag.Bool("v", false, "Enable debug logging")
	requestLogging = flag.Bool("request-logging", false, "Enable request logging")

	showVersion = flag.Bool("version", false, "Print version information and exit")

	// Cluster configuration flags
	clusterEnabled = flag.Bool("cluster-enabled", false, "Enable cluster mode")
	nodeID         = flag.String("node-id", "", "Unique node identifier (required in cluster mode)")
	clusterAddr    = flag.String("cluster-addr", ":7000", "Address for cluster communication")
	seedsStr       = flag.String("seeds", "", "Comma-separated list of seed nodes (e.g., node1:7000,node2:7000 or ocache.svc.cluster.local:7000)")
	advertiseAddr  = flag.String("advertise-addr", "", "Address advertised to other nodes for routing (defaults to -listen-addr)")

	seeds []string
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
func initializeCluster(ctx context.Context) *coordinator.Coordinator {
	if !AppConfig.ClusterEnabled {
		return nil
	}

	coordConfig := &coordinator.Config{
		Enabled:       true,
		MyNodeID:      AppConfig.NodeID,
		ClusterAddr:   AppConfig.ClusterAddr,
		ListenAddr:    AppConfig.ListenAddr,
		AdvertiseAddr: AppConfig.AdvertiseAddr,
		Seeds:         AppConfig.Seeds,
		DiskPath:      AppConfig.DiskPath,
	}

	var err error
	coord, err := coordinator.New(coordConfig)
	if err != nil {
		zlog.Fatal().Err(err).Msg("Failed to create coordinator")
	}

	if err := coord.Start(ctx); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to start coordinator")
	}

	zlog.Info().
		Str("node_id", AppConfig.NodeID).
		Str("cluster_addr", AppConfig.ClusterAddr).
		Int("seeds", len(AppConfig.Seeds)).
		Msg("Cluster coordinator started")

	return coord
}

// initializeStorage sets up the storage layer
func initializeStorage() *stor.Storage {
	storageConfig := &stor.StorageConfig{
		DiskPath:               AppConfig.DiskPath,
		TTL:                    AppConfig.TTL,
		InlineThreshold:        AppConfig.InlineThreshold,
		CompactThreshold:       AppConfig.CompactThreshold,
		SegmentSize:            AppConfig.SegmentSize,
		FdCacheSize:            AppConfig.FdCacheSize,
		MaxDiskUsage:           AppConfig.MaxDiskUsage,
		CompactionThreads:      AppConfig.CompactionThreads,
		FragThreshold:          AppConfig.FragThreshold,
		MinSegmentAge:          AppConfig.RecompactMinSegmentAge,
		MinSegments:            AppConfig.RecompactMinSegments,
		DisableRecompaction:    AppConfig.RecompactDisable,
		CleanupInterval:        AppConfig.TTLCleanupInterval,
		MetadataCacheSize:      AppConfig.MetadataCacheSize,
		MetadataBackgroundJobs: AppConfig.MetadataBackgroundJobs,
		RecoveryWorkers:        AppConfig.RecoveryWorkers,
		DeleteBatchSize:        AppConfig.DeleteBatchSize,
	}

	s, err := stor.NewStorageWithConfig(storageConfig)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to open storage")
	}

	return s
}

// startUserServices starts the user-facing gRPC and HTTP gateway services
func startUserServices(coord *coordinator.Coordinator, storage *stor.Storage) {
	go service.StartGRPCServer(coord, storage, *listenAddr, *requestLogging) // Start gRPC server in goroutine
	go service.StartGRPCGatewayServer(coord, *listenAddr, *listenHTTP)       // Start grpc-gateway on different address
}

// waitForShutdown waits for shutdown signal or coordinator error
func waitForShutdown(coord *coordinator.Coordinator) {
	// Handle graceful shutdown on SIGINT/SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create coordinator error channel if coordinator is enabled
	var coordErrCh <-chan error
	if coord != nil {
		coordErrCh = coord.ErrorChan()
	}

	// Wait for shutdown signal or fatal coordinator error
	select {
	case sig := <-sigChan:
		zlog.Info().Str("signal", sig.String()).Msg("Received shutdown signal, shutting down...")
	case err := <-coordErrCh:
		if err != nil {
			zlog.Error().Err(err).Msg("Coordinator reported fatal error, shutting down...")
		}
	}
}

// performShutdown handles graceful shutdown of all components
func performShutdown(cancel context.CancelFunc, coord *coordinator.Coordinator, storage *stor.Storage) {
	// Close coordinator if enabled - this handles AnnounceLeaving() BEFORE dskit shuts down
	if coord != nil {
		if err := coord.Stop(); err != nil {
			zlog.Error().Err(err).Msg("Error stopping coordinator")
		}
	}

	// NOW cancel the context - this is for cleanup of any remaining goroutines
	// that might be watching the context. The coordinator's graceful shutdown
	// is already complete at this point.
	cancel()

	// Close storage (flush segments, close RocksDB, etc.)
	storage.Close()

	zlog.Info().Msg("Shutdown complete")
}

func RunServer() {
	// Create a context for the server
	// Note: We don't defer cancel() here - it's called explicitly in performShutdown().
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize Prometheus metrics
	metrics.Init()
	zlog.Info().Msg("Prometheus metrics initialized")

	// Set up internal cluster communication if enabled
	coord := initializeCluster(ctx)

	// Initialize the storage layer
	storage := initializeStorage()

	// Start gRPC and HTTP gateway for client requests
	startUserServices(coord, storage)

	// Wait for shutdown signal
	waitForShutdown(coord)

	// Perform graceful shutdown (includes cancel() after coord.Stop())
	performShutdown(cancel, coord, storage)
}

func main() {
	flag.Parse()

	// Print version and exit if requested
	if *showVersion {
		fmt.Printf("ocache version %s\n", Version)
		fmt.Printf("  commit: %s\n", Commit)
		fmt.Printf("  built:  %s\n", BuildTime)
		os.Exit(0)
	}

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
