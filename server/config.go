package main

// Config holds all configuration options for the cache service. It's populated
// once at start-up via LoadConfig() and accessed via the global variable
// `AppConfig` from the rest of the codebase.
//

type Config struct {
	DiskPath  string // Directory for on-disk cache data
	Threshold int    // Threshold for small objects (bytes)
	TTL       int    // Default TTL (seconds)
	Port      int    // gRPC listen port
	HTTPPort  int    // HTTP (grpc-gateway) port
	Verbose   bool   // Enable verbose/debug logging
}

// AppConfig is the singleton that stores the parsed configuration.
var AppConfig Config

// LoadConfig must be invoked after flag.Parse() so that the package-level
// flag variables in main.go are initialised. It copies the values from those
// flag variables into the global AppConfig variable.
func LoadConfig() {
	AppConfig = Config{
		DiskPath:  *diskPath,
		Threshold: *threshold,
		TTL:       *ttl,
		Port:      *port,
		HTTPPort:  *httpPort,
		Verbose:   *verbose,
	}
}
