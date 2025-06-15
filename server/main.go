package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	stor "github.com/tigrisdata/cache_service/server/storage"
)

var (
	diskPath  = flag.String("disk", "/var/cache", "Directory for disk cache")
	threshold = flag.Int("threshold", 256*1024, "Small obj threshold")
	ttl       = flag.Int("ttl", 900, "Default TTL in seconds")
	port      = flag.Int("port", 9000, "Listen port")
	httpPort  = flag.Int("http-port", 9001, "HTTP port")
	verbose   = flag.Bool("v", false, "Enable debug logging")
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Status() int {
	if rw.status == 0 {
		return 200
	}
	return rw.status
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	return rw.ResponseWriter.Write(b)
}

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
	stor.InitStorage(AppConfig.DiskPath, AppConfig.TTL, AppConfig.Threshold)

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
