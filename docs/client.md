# Client Documentation

OCache provides multiple client options for interacting with the cache service:

- **Go Client Library** - A native Go library for programmatic access
- **CLI Client** - Command-line interface for manual operations and scripting

## Go Client Library

The Go client library provides automatic mode detection, smart routing for clusters, and connection pooling for optimal performance. It seamlessly works with both single-node and cluster deployments.

### Key Features

- **Auto Mode Detection**: Automatically detects cluster vs single-node deployments
- **Smart Routing**: Uses consistent hashing to route requests in cluster mode
- **Connection Pooling**: Maintains multiple connections per node for better load distribution
- **Automatic Topology Refresh**: Periodically updates cluster topology to handle node changes
- **Fallback Routing**: Falls back to round-robin when smart routing is unavailable
- **Retry Logic**: Automatically retries on routing errors with topology refresh

### Connection Modes

The client supports three connection modes:

#### Auto Mode (Default)

- Attempts to detect cluster topology service
- Falls back to simple mode if not available
- Best for most use cases

#### Simple Mode

- Direct connections to provided addresses
- Hash-based routing for multiple addresses
- No topology discovery
- Good for single nodes or basic multi-server setups

#### Cluster Mode

- Requires topology service
- Smart routing with consistent hashing
- Automatic topology refresh
- Best for production clusters

### Configuration

The client supports flexible configuration through the ClientConfig structure:

```go
type ClientConfig struct {
    // List of server addresses (single or multiple)
    Addrs []string

    // Connection mode: "auto", "simple", or "cluster"
    // - auto: Automatically detect mode (default)
    // - simple: Direct connections without topology
    // - cluster: Use topology service for smart routing
    Mode ConnectionMode

    // Number of connections per address
    // Used differently based on mode:
    // - Simple mode: connections per provided address
    // - Cluster mode: connections per discovered node
    ConnectionPoolSize int

    // How often to refresh cluster topology (cluster mode only)
    // Default: 30s
    RefreshInterval time.Duration

    // Optional gRPC dial options for custom configuration
    DialOpts []grpc.DialOption
}
```

### Usage Examples

#### Auto Mode (Recommended)

```go
package main

import (
    "context"
    "fmt"
    "log"

    cacheclient "github.com/tigrisdata/ocache/client"
)

func main() {
    // Client auto-detects single node or cluster mode
    config := &cacheclient.ClientConfig{
        Addrs: []string{"localhost:9000"},
    }

    client, err := cacheclient.NewWithConfig(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Operations work seamlessly regardless of mode
    err = client.Put(ctx, "key", []byte("value"), 3600)
    if err != nil {
        log.Fatal(err)
    }

    value, err := client.Get(ctx, "key")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Value: %s\n", string(value))
    fmt.Printf("Mode: %s\n", client.GetMode())
}
```

#### Cluster Mode with Custom Configuration

```go
package main

import (
    "context"
    "fmt"
    "time"

    cacheclient "github.com/tigrisdata/ocache/client"
)

func main() {
    // Explicitly configure for cluster mode
    config := &cacheclient.ClientConfig{
        // Multiple seed addresses for discovery
        Addrs: []string{
            "localhost:9001",
            "localhost:9002",
            "localhost:9003",
        },
        ConnectionPoolSize: 10,                // 10 connections per node
        RefreshInterval:    15 * time.Second,  // Frequent topology updates
    }

    // Create cluster-aware client
    client, err := cacheclient.NewWithConfig(config)
    if err != nil {
        panic(fmt.Sprintf("Failed to create cluster client: %v", err))
    }
    defer client.Close()

    ctx := context.Background()

    // Put a value - automatically routed to appropriate node
    err = client.Put(ctx, "my-key", []byte("my-value"), 3600)
    if err != nil {
        fmt.Printf("Failed to put: %v\n", err)
        return
    }

    // Get the value - routed to same node as Put
    value, err := client.Get(ctx, "my-key")
    if err != nil {
        fmt.Printf("Failed to get: %v\n", err)
        return
    }

    fmt.Printf("Retrieved value: %s\n", string(value))
}
```

### Operations

All operations support automatic routing and connection pooling:

#### Basic Operations

```go
var err error

// Put a value
err = client.Put(ctx, key, data, ttlSeconds)

// Get a value
data, err = client.Get(ctx, key)

// Get byte range
data, err = client.GetRange(ctx, key, start, end)

// Delete a value
err = client.Delete(ctx, key)

// List keys with prefix
keys, err = client.List(ctx, prefix)
```

#### Streaming Operations

For large values, use streaming operations:

```go
var err error

// Stream data to cache
err = client.PutStream(ctx, key, reader, ttlSeconds)

// Stream data from cache
err = client.GetStream(ctx, key, writer)

// Stream byte range
err = client.GetRangeStream(ctx, key, start, end, writer)
```

### Routing Behavior

#### Smart Routing

1. Key is hashed to determine partition
2. Partition owner (node) is identified
3. Request sent to appropriate node's connection pool
4. Pool selects connection using round-robin

#### Fallback Routing

When smart routing fails (e.g., topology not available):

1. Falls back to round-robin node selection
2. Maintains request distribution across all nodes
3. Continues attempting topology refresh

#### Retry Logic

On routing errors:

1. Automatically refreshes topology
2. Retries request once with updated routing
3. Returns error if retry fails

### Error Handling

The client handles various error scenarios:

- **Connection failures**: Pool removes unhealthy connections
- **Routing errors**: Triggers topology refresh and retry
- **Node failures**: Requests routed to remaining nodes
- **Topology changes**: Automatic discovery and adaptation

### Monitoring

You can monitor cluster client behavior:

```go
// Get list of connected nodes
nodes := client.GetConnectedNodes()

// Get node responsible for a key
nodeID, err := client.GetNodeForKey(key)
```

### Best Practices

1. **Initialize once**: Create a single ClusterClient instance and reuse it
2. **Tune pool size**: Adjust based on your concurrency requirements
3. **Handle errors**: Implement appropriate retry logic for your use case
4. **Close cleanly**: Always call `Close()` to release resources

### Configuration Examples

#### High Throughput Configuration

```go
config := &cacheclient.ClientConfig{
    Addrs:              seedNodes,
    ConnectionPoolSize: 20,                // High pool size for throughput
    RefreshInterval:    10 * time.Second,  // Frequent updates
}
```

#### Resource-Constrained Configuration

```go
config := &cacheclient.ClientConfig{
    Addrs:              seedNodes,
    ConnectionPoolSize: 2,                 // Minimal connections
    RefreshInterval:    60 * time.Second,  // Less frequent updates
}
```

#### Development/Testing Configuration

```go
config := &cacheclient.ClientConfig{
    Addrs:              []string{"localhost:9001"},
    Mode:               cacheclient.ModeSimple,  // Direct connection
    ConnectionPoolSize: 1,                        // Single connection for debugging
}
```
