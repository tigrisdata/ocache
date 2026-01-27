# Embedded Client Documentation

The embedded client allows you to embed OCache directly within your Go application, eliminating the need for a separate cache server process. This is ideal for services that want local storage with optional cluster-wide coordination.

## Overview

The embedded client provides:

- **Direct Storage Access**: No network overhead for local operations
- **Full Cluster Support**: Optional cluster mode with automatic routing
- **Shared Interface**: Implements the same `CacheClient` interface as the gRPC client
- **Metrics & Logging**: Built-in observability with the same metrics as the server
- **Cluster-Wide List**: K-way merge across all nodes for list operations

## Installation

Add the embedded module to your Go project:

```go
require github.com/tigrisdata/ocache/embedded v1.x.x
```

## Configuration

### Config Structure

```go
type Config struct {
    // DiskPath is the path to the cache data directory (required)
    DiskPath string

    // TTL is the default time-to-live for cache entries (required)
    TTL time.Duration

    // MaxDiskUsage is the maximum disk usage in bytes (0 = unlimited)
    MaxDiskUsage int64

    // InlineThreshold is the size threshold for inline vs file storage (default: 64KB)
    // Objects smaller than this are stored in RocksDB, larger ones as files.
    InlineThreshold int

    // NodeID is the unique identifier for this node in cluster mode
    NodeID string

    // ClusterAddr is the address for cluster membership (gossip) protocol
    // Example: ":7000"
    ClusterAddr string

    // GRPCAddr is the address for the gRPC server to listen on
    // Example: ":9000"
    GRPCAddr string

    // AdvertiseAddr is the address advertised to other nodes for gRPC connections
    // Example: "node1.cluster:9000"
    AdvertiseAddr string

    // SeedNodes is a list of seed nodes for cluster discovery
    // Example: []string{"node1:7000", "node2:7000"}
    SeedNodes []string
}
```

### Required Fields

- `DiskPath`: Where to store cache data on disk
- `TTL`: Default time-to-live for entries

### Cluster Mode Fields

To enable cluster mode, provide:
- `NodeID`: Unique identifier for this node
- `ClusterAddr`: Address for memberlist gossip protocol
- `GRPCAddr`: Address for gRPC server (other nodes route requests here)
- `AdvertiseAddr`: Address other nodes should use to connect
- `SeedNodes`: Initial nodes for cluster discovery

## Usage Examples

### Single-Node Mode

The simplest setup runs as a standalone cache:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/tigrisdata/ocache/embedded"
)

func main() {
    // Create embedded cache (single-node mode)
    cache, err := embedded.New(&embedded.Config{
        DiskPath:     "/var/cache/myapp",
        TTL:          time.Hour,
        MaxDiskUsage: 100 * 1024 * 1024 * 1024, // 100GB
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    ctx := context.Background()

    // Put a value
    err = cache.Put(ctx, "user:123", []byte(`{"name":"Alice"}`), 3600)
    if err != nil {
        log.Fatal(err)
    }

    // Get a value
    data, err := cache.Get(ctx, "user:123")
    if err != nil {
        log.Fatal(err)
    }
    if data != nil {
        log.Printf("Got: %s", string(data))
    }

    // List keys
    keys, err := cache.List(ctx, "user:")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Found %d user keys", len(keys))

    // Delete a key
    err = cache.Delete(ctx, "user:123")
    if err != nil {
        log.Fatal(err)
    }
}
```

### Cluster Mode

For distributed caching across multiple instances:

```go
package main

import (
    "context"
    "log"
    "os"
    "strings"
    "time"

    "github.com/tigrisdata/ocache/embedded"
)

func main() {
    // Get configuration from environment
    nodeID := os.Getenv("NODE_ID")           // e.g., "pod-0"
    clusterAddr := os.Getenv("CLUSTER_ADDR") // e.g., ":7000"
    grpcAddr := os.Getenv("GRPC_ADDR")       // e.g., ":9000"
    advertiseAddr := os.Getenv("ADVERTISE")  // e.g., "pod-0.svc:9000"
    seeds := strings.Split(os.Getenv("SEEDS"), ",") // e.g., "pod-0.svc:7000,pod-1.svc:7000"

    // Create embedded cache with cluster support
    cache, err := embedded.New(&embedded.Config{
        DiskPath:      "/var/cache/myapp",
        TTL:           time.Hour,
        MaxDiskUsage:  100 * 1024 * 1024 * 1024, // 100GB
        NodeID:        nodeID,
        ClusterAddr:   clusterAddr,
        GRPCAddr:      grpcAddr,
        AdvertiseAddr: advertiseAddr,
        SeedNodes:     seeds,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    // Start gRPC server for cluster routing
    if err := cache.StartGRPCServer(); err != nil {
        log.Fatal(err)
    }

    // Wait for cluster to form
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := cache.WaitReady(ctx); err != nil {
        log.Fatal("Cluster not ready:", err)
    }

    log.Printf("Cache ready in %s mode", cache.GetMode())
    log.Printf("Connected nodes: %v", cache.GetConnectedNodes())

    // Use the cache - operations are automatically routed
    ctx = context.Background()

    // Put routes to the node owning this key
    err = cache.Put(ctx, "session:abc", []byte("data"), 1800)
    if err != nil {
        log.Fatal(err)
    }

    // Get routes to the same node
    data, err := cache.Get(ctx, "session:abc")
    if err != nil {
        log.Fatal(err)
    }

    // List returns keys from ALL nodes (K-way merge)
    keys, err := cache.List(ctx, "session:")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Found %d sessions across cluster", len(keys))
}
```

## Operations

### Basic Operations

```go
var err error
ctx := context.Background()

// Put with TTL in seconds
err = cache.Put(ctx, key, data, ttlSeconds)

// Get returns nil, nil for not found
data, err = cache.Get(ctx, key)

// Delete
err = cache.Delete(ctx, key)

// List all keys with prefix (cluster-wide)
keys, err = cache.List(ctx, prefix)

// Paginated list
keys, nextToken, hasMore, err = cache.ListPage(ctx, prefix, limit, continuationToken)
```

### Streaming Operations

For large values, use streaming to avoid loading everything into memory:

```go
// Stream data to cache
file, _ := os.Open("large-file.bin")
defer file.Close()
err = cache.PutStream(ctx, "artifact:build123", file, 86400)

// Stream data from cache
output, _ := os.Create("output.bin")
defer output.Close()
err = cache.GetStream(ctx, "artifact:build123", output)

// Get byte range
data, err = cache.GetRange(ctx, "artifact:build123", 0, 1024)

// Stream byte range
err = cache.GetRangeStream(ctx, "artifact:build123", 0, 1024, output)
```

## Cluster Routing

In cluster mode, operations are automatically routed:

### Local vs Remote Keys

- Keys hashed to the local node are served directly from storage
- Keys hashed to remote nodes are forwarded via gRPC
- The client handles all routing transparently

### List Operations

The `List` and `ListPage` methods perform cluster-wide queries:

1. Query all active nodes in parallel
2. Perform K-way merge of sorted results
3. Return deduplicated, sorted keys
4. Support pagination with continuation tokens

### Readiness

Check if the client is ready:

```go
// Blocking wait with timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err := cache.WaitReady(ctx)

// Non-blocking check
if cache.IsReady() {
    // Safe to serve requests
}
```

## Advanced Usage

### Accessing Underlying Layers

For advanced use cases, you can access the underlying components:

```go
// Direct access to operations layer (includes routing)
ops := cache.Operations()
reader, found, err := ops.Get(ctx, key, 0, 0)

// Direct access to local storage (no routing)
storage := cache.Storage()

// Access coordinator for cluster information
coord := cache.Coordinator()
if coord != nil {
    ring := coord.GetRing()
    nodes := ring.GetActiveNodes()
}

// Access gRPC service for custom handlers
svc := cache.Service()

// Get the gRPC server instance
server := cache.GetGRPCServer()
```

### Custom gRPC Configuration

Register additional gRPC services alongside the cache:

```go
cache, _ := embedded.New(config)

// Start gRPC server
cache.StartGRPCServer()

// Register additional services on the same server
myService := &MyCustomService{}
mypb.RegisterMyServiceServer(cache.GetGRPCServer(), myService)
```

## Best Practices

### Storage Configuration

```go
config := &embedded.Config{
    DiskPath:     "/var/cache/myapp",
    TTL:          time.Hour,
    MaxDiskUsage: 100 * 1024 * 1024 * 1024, // 100GB limit

    // Tune inline threshold based on your data:
    // - Smaller values: More files, better for large objects
    // - Larger values: More RocksDB usage, better for small objects
    InlineThreshold: 64 * 1024, // 64KB default
}
```

### Cluster Mode

1. **Use stable node IDs**: In Kubernetes, use StatefulSet pod names
2. **Include all nodes as seeds**: Ensures cluster formation
3. **Wait for readiness**: Call `WaitReady()` before serving traffic
4. **Advertise reachable addresses**: Ensure `AdvertiseAddr` is routable from other nodes

### Resource Management

1. **Always defer Close()**: Ensures clean shutdown
2. **Use streaming for large objects**: Avoid memory pressure
3. **Set MaxDiskUsage**: Prevent disk exhaustion
4. **Monitor metrics**: The embedded client exposes Prometheus metrics

### Error Handling

```go
data, err := cache.Get(ctx, key)
if err != nil {
    // Handle error (network, storage, etc.)
    return err
}
if data == nil {
    // Key not found - this is not an error
    return ErrNotFound
}
// Use data
```

## Monitoring

The embedded client exposes the same Prometheus metrics as the standalone server:

- `ocache_rpc_requests_total` - Total RPC requests by method and status
- `ocache_rpc_duration_milliseconds` - RPC latency histogram
- `ocache_storage_operations_total` - Storage operations by type
- `ocache_streams_active` - Currently active streaming operations
- `ocache_stream_bytes_transferred_total` - Bytes transferred via streaming

## Comparison: Embedded vs gRPC Client

| Feature | Embedded Client | gRPC Client |
|---------|----------------|-------------|
| Deployment | In-process | Separate server |
| Local access | Direct storage | Network hop |
| Cluster support | Yes | Yes |
| Storage management | Your responsibility | Server handles |
| Memory usage | Higher (storage in-process) | Lower (data on server) |
| Best for | Services needing local cache | Services connecting to shared cache |

## Migration from gRPC Client

The embedded client implements the same `CacheClient` interface, making migration straightforward:

```go
// Before: gRPC client
client, _ := cacheclient.NewWithConfig(&cacheclient.ClientConfig{
    Addrs: []string{"localhost:9000"},
})

// After: Embedded client
client, _ := embedded.New(&embedded.Config{
    DiskPath: "/var/cache/myapp",
    TTL:      time.Hour,
})

// Same interface - code works unchanged
err := client.Put(ctx, key, data, ttl)
data, err := client.Get(ctx, key)
```
