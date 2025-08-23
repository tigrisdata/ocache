package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
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

	port           = flag.Int("port", 9000, "Listen port")
	httpPort       = flag.Int("http-port", 9001, "HTTP port")
	verbose        = flag.Bool("v", false, "Enable debug logging")
	requestLogging = flag.Bool("request-logging", false, "Enable request logging")
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

func RunServer() {
	// Initialize Prometheus metrics
	metrics.Init()
	zlog.Info().Msg("Prometheus metrics initialized")

	// Create storage config from AppConfig
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
	}
	stor.InitStorageWithConfig(storageConfig)

	grpcAddr := fmt.Sprintf(":%d", *port)
	go startGRPCServer()                           // Start gRPC server in goroutine
	go startGRPCGatewayServer(grpcAddr, *httpPort) // Start grpc-gateway on different port

	// Handle graceful shutdown on SIGINT/SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	zlog.Info().Str("signal", sig.String()).Msg("Received shutdown signal, shutting down...")

	// Close storage (flush segments, close RocksDB, etc.)
	stor.CloseStorage()

	zlog.Info().Msg("Shutdown complete")
}

func main() {
	flag.Parse()
	LoadConfig()
	configureLogger()

	RunServer() // Initialize and run the server
}
