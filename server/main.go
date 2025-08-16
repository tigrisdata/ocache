package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	stor "github.com/tigrisdata/ocache/storage"
)

var (
	diskPath         = flag.String("disk", "/var/cache", "Directory for disk cache")
	inlineThreshold  = flag.Int("threshold", 64*1024, "Small object threshold (bytes) that are inlined with metadata")
	compactThreshold = flag.Int64("compact-threshold", 16*1024*1024, "Compaction threshold (bytes)")
	segmentSize      = flag.Int64("segment-size", 256*1024*1024, "Segment size (bytes)")
	ttl              = flag.Int("ttl", 0, "Default TTL in seconds when no key-level TTL is set")
	port             = flag.Int("port", 9000, "Listen port")
	httpPort         = flag.Int("http-port", 9001, "HTTP port")
	verbose          = flag.Bool("v", false, "Enable debug logging")
	fdCacheSize      = flag.Int("fd-cache-size", 10000, "Size of the file descriptor cache (entries)")
	maxDiskUsage     = flag.Int64("max-disk-usage", 0, "Maximum disk usage in bytes (0 = unlimited, uses LRU eviction)")
)

func configureLogger() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if AppConfig.Verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func RunServer() {
	stor.InitStorage(
		AppConfig.DiskPath,
		AppConfig.TTL,
		AppConfig.InlineThreshold,
		AppConfig.CompactThreshold,
		AppConfig.SegmentSize,
		AppConfig.FdCacheSize,
		AppConfig.MaxDiskUsage,
	)

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
