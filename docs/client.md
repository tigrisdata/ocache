# Client Documentation

OCache provides multiple client options for interacting with the cache service:

- **Go Client Library** - A native Go library for programmatic access
- **CLI Client** - Command-line interface for manual operations and scripting

## Go Client Library

The ClusterClient is a cluster-aware cache client that provides smart routing based on consistent hashing and partition ownership information. It automatically distributes requests to the appropriate nodes in the cluster and uses connection pooling for improved performance and fault tolerance.

### Key Features

- **Smart Routing**: Uses consistent hashing to route requests to the correct node
- **Connection Pooling**: Maintains multiple connections per node for better load distribution
- **Automatic Topology Refresh**: Periodically updates cluster topology to handle node changes
- **Fallback Routing**: Falls back to round-robin when smart routing is unavailable
- **Retry Logic**: Automatically retries on routing errors with topology refresh

### Configuration

The ClusterClient requires a configuration object that specifies connection parameters:

```go
type ClusterClientConfig struct {
    // List of seed node addresses to bootstrap the client
    SeedAddrs []string

    // Number of connections to maintain per node (must be > 0)
    PoolSizePerNode int

    // How often to refresh cluster topology (default: 30s)
    TopologyRefreshInterval time.Duration

    // Optional gRPC dial options
    DialOpts []grpc.DialOption
}
```

### Usage Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    cacheclient "github.com/tigrisdata/ocache/client"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    // Create configuration for the cluster client
    config := &cacheclient.ClusterClientConfig{
        // Seed addresses for initial topology discovery
        SeedAddrs: []string{
            "localhost:9001",
            "localhost:9002",
            "localhost:9003",
        },

        // Number of connections per node
        // For 3 nodes with pool size 5 = 15 total connections
        PoolSizePerNode: 5,

        // Topology refresh interval
        TopologyRefreshInterval: 30 * time.Second,

        // gRPC options
        DialOpts: []grpc.DialOption{
            grpc.WithTransportCredentials(insecure.NewCredentials()),
            grpc.WithDefaultCallOptions(
                grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
                grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
            ),
        },
    }

    // Create cluster client with connection pooling
    client, err := cacheclient.NewClusterClient(config)
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

### Connection Pool Size Guidelines

Choosing the right pool size depends on your workload:

- **Low concurrency (1-10 concurrent requests)**: 2-3 connections per node
- **Medium concurrency (10-50 concurrent requests)**: 5-10 connections per node
- **High concurrency (50+ concurrent requests)**: 10-20 connections per node

Total connections = Number of nodes × Pool size per node

### Operations

All operations support automatic routing and connection pooling:

#### Basic Operations

```go
// Put a value
err := client.Put(ctx, key, data, ttlSeconds)

// Get a value
data, err := client.Get(ctx, key)

// Delete a value
err := client.Delete(ctx, key)

// List keys with prefix
keys, err := client.List(ctx, prefix)
```

#### Streaming Operations

For large values, use streaming operations:

```go
// Stream data to cache
err := client.PutStream(ctx, key, reader, ttlSeconds)

// Stream data from cache
err := client.GetStream(ctx, key, writer)

// Get byte range
data, err := client.GetRange(ctx, key, start, end)

// Stream byte range
err := client.GetRangeStream(ctx, key, start, end, writer)
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
config := &cacheclient.ClusterClientConfig{
    SeedAddrs:               seedNodes,
    PoolSizePerNode:         20, // High pool size for throughput
    TopologyRefreshInterval: 10 * time.Second, // Frequent updates
}
```

#### Resource-Constrained Configuration

```go
config := &cacheclient.ClusterClientConfig{
    SeedAddrs:               seedNodes,
    PoolSizePerNode:         2, // Minimal connections
    TopologyRefreshInterval: 60 * time.Second, // Less frequent updates
}
```

#### Development/Testing Configuration

```go
config := &cacheclient.ClusterClientConfig{
    SeedAddrs:               []string{"localhost:9001"},
    PoolSizePerNode:         1, // Single connection for debugging
    TopologyRefreshInterval: 5 * time.Second, // Fast feedback
}
```
