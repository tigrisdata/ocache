package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	stor "github.com/tigrisdata/cache_service/server/storage"
)

var (
	diskPath    = flag.String("disk", "/var/cache", "Directory for disk cache")
	threshold   = flag.Int("threshold", 256*1024, "Small obj threshold (bytes)")
	ttl         = flag.Int("ttl", 900, "Default TTL in seconds")
	port        = flag.Int("port", 9000, "Listen port")
	httpPort    = flag.Int("http-port", 9001, "HTTP port")
	verbose     = flag.Bool("v", false, "Enable debug logging")
	fdCacheSize = flag.Int("fd-cache-size", 1000, "Size of the file descriptor cache (entries)")
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
	stor.InitStorage(AppConfig.DiskPath, AppConfig.TTL, AppConfig.Threshold, AppConfig.FdCacheSize)

	grpcAddr := fmt.Sprintf(":%d", *port)
	go startGRPCServer()                           // Start gRPC server in goroutine
	go startGRPCGatewayServer(grpcAddr, *httpPort) // Start grpc-gateway on different port

	select {} // Block forever
}

func main() {
	flag.Parse()
	LoadConfig()
	configureLogger()

	RunServer() // Initialize and run the server
}
