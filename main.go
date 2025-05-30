package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var (
	diskPath  = flag.String("disk", "/var/cache", "Directory for disk cache")
	threshold = flag.Int("threshold", 128*1024, "Small obj threshold")
	ttl       = flag.Int("ttl", 900, "Default TTL in seconds")
	port      = flag.Int("port", 8080, "Listen port")
	verbose   = flag.Bool("v", false, "Enable debug logging")
)

func GetDiskPath() string { return *diskPath }
func GetThreshold() int   { return *threshold }
func GetTTL() int         { return *ttl }
func GetPort() int        { return *port }

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

// loggingMiddleware logs each HTTP request using zerolog
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
		duration := time.Since(start)

		zlog.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("remote", r.RemoteAddr).
			Int("status", rw.Status()).
			Dur("duration", duration).
			Msg("request completed")
	})
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	flag.Parse()

	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := os.MkdirAll(GetDiskPath(), 0o755); err != nil {
		zlog.Fatal().Err(err).Msg("failed to create disk path")
	}

	initStorage(GetDiskPath(), GetTTL())

	mux := http.NewServeMux()
	mux.HandleFunc("/put", handlePut)
	mux.HandleFunc("/get", handleGet)
	mux.HandleFunc("/delete", handleDelete)
	mux.HandleFunc("/list", handleList)

	// Wrap mux with logging middleware and h2c (HTTP/2 cleartext)
	handler := loggingMiddleware(mux)
	h2cHandler := h2c.NewHandler(handler, &http2.Server{})
	err := http.ListenAndServe(fmt.Sprintf(":%d", GetPort()), h2cHandler)
	if err != nil {
		zlog.Fatal().Err(err).Msg("server failed to start")
	}
}
