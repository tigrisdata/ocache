# CLI Usage Guide

## Overview

The `ocachecli` command-line tool provides a convenient interface for interacting with the OCache service. It features a unified client architecture with automatic mode detection, connection pooling, and smart routing capabilities.

## Installation

Build the CLI from source:

```bash
make build-cli
```

This creates the `ocachecli` executable in the project root.

## Connection Modes

The CLI supports three connection modes that determine how it connects to cache servers:

### Auto Mode (Default)

Automatically detects whether to use cluster or simple mode by checking for topology service availability:

```bash
# Auto-detects the appropriate mode
ocachecli --addr localhost:9000 <command>

# Multiple servers - will detect if cluster topology is available
ocachecli --addr "node1:9001,node2:9002,node3:9003" <command>
```

### Simple Mode

Direct connections without topology service. Uses hash-based routing for multiple servers:

```bash
# Force simple mode
ocachecli --mode simple --addr "server1:9000,server2:9000" <command>
```

### Cluster Mode

Uses topology service for smart routing with consistent hashing:

```bash
# Force cluster mode (requires topology service)
ocachecli --mode cluster --addr "node1:9001,node2:9002" <command>
```

## Global Flags

| Flag                 | Description                                                    | Default          |
| -------------------- | -------------------------------------------------------------- | ---------------- |
| `--addr`             | Cache server address(es), comma-separated for multiple servers | `localhost:9000` |
| `--mode`             | Connection mode: `auto`, `simple`, or `cluster`                | `auto`           |
| `--topology-refresh` | Topology refresh interval (cluster mode only)                  | `30s`            |

## Commands

### Put

Store a value in the cache:

```bash
# Put with value as argument
ocachecli put mykey "my value"

# Put with TTL
ocachecli put mykey "my value" --ttl 3600

# Put from stdin (useful for large files)
cat file.txt | ocachecli put mykey

# Multiple servers with auto-detection
ocachecli --addr "node1:9001,node2:9002" put mykey "value"

# Force specific mode
ocachecli --mode cluster --addr "node1:9001" put mykey "value"
```

### Get

Retrieve a value from the cache:

```bash
# Get a value
ocachecli get mykey

# Multiple servers
ocachecli --addr "node1:9001,node2:9002" get mykey

# Save to file
ocachecli get mykey > output.txt
```

### Delete

Remove a key from the cache:

```bash
# Delete a key
ocachecli del mykey

# Delete with multiple servers
ocachecli --addr "node1:9001,node2:9002" del mykey
```

### List

List keys in the cache:

```bash
# List all keys
ocachecli list

# List with prefix filter
ocachecli list --prefix "user:"

# Multiple servers
ocachecli --addr "node1:9001,node2:9002" list --prefix "session:"
```

### Cluster

Inspect cluster topology and key ownership. These commands only work when connected to a cluster-enabled server.

#### Topology

Display full cluster topology including nodes and ring configuration:

```bash
# Display cluster topology
ocachecli cluster topology

# Output in JSON format
ocachecli cluster topology --json
```

Example output:
```
Cluster Topology (Epoch: 12345678901234567890)

Ring Configuration:
  Replication Factor: 1
  Total Tokens: 384

Nodes:
NODE ID   STATUS   LISTEN ADDRESS    CLUSTER ADDRESS   JOINED AT
-------   ------   --------------    ---------------   ---------
node1     ACTIVE   localhost:9001    localhost:7001    2025-01-09T10:30:00Z
node2     ACTIVE   localhost:9002    localhost:7002    2025-01-09T10:30:05Z
node3     ACTIVE   localhost:9003    localhost:7003    2025-01-09T10:30:10Z
```

#### Node

Get the node that owns a specific key:

```bash
# Find which node owns a key
ocachecli cluster node mykey

# Output in JSON format
ocachecli cluster node mykey --json
```

Example output:
```
Key: mykey
Node: node2
Address: localhost:9002
```

#### Epoch

Display the current topology epoch:

```bash
# Display epoch
ocachecli cluster epoch

# Output in JSON format
ocachecli cluster epoch --json
```

Example output:
```
Epoch: 12345678901234567890
```

#### Cluster Command Flags

| Flag     | Description              | Default |
| -------- | ------------------------ | ------- |
| `--json` | Output in JSON format    | `false` |

## Mode Detection and Behavior

### Auto Mode Behavior

When using the default `auto` mode:

1. The client attempts to connect to the provided addresses
2. It checks if a cluster topology service is available
3. If topology service is found → operates in cluster mode
4. If no topology service → operates in simple mode

### Simple Mode Features

- Direct connections to all provided addresses
- Each address gets its own connection pool
- Hash-based routing distributes keys across servers
- No automatic failover (relies on gRPC retries)
- Best for standalone servers or simple multi-server setups

### Cluster Mode Features

- Fetches and maintains cluster topology
- Smart routing based on consistent hashing
- Automatic topology refresh at configured intervals
- Handles node additions/removals gracefully
- Partition-aware routing ensures keys go to correct nodes
- Best for production clusters with coordinator service

## Connection Pooling

The CLI uses connection pooling for better performance:

- **Benefits**:
  - Better load distribution
  - Reduced connection setup overhead
  - Higher throughput for concurrent operations
  - Resilience to individual connection failures

## Examples

### Single Server Operations

```bash
# Auto mode will detect simple mode for single server
ocachecli --addr localhost:9000 put mykey "value"

# Store a configuration file
cat config.json | ocachecli put app:config --ttl 86400

# Retrieve configuration
ocachecli get app:config

# List all app configurations
ocachecli list --prefix "app:"

# Delete old configuration
ocachecli del app:config:old
```

### Multi-Server Operations

```bash
# Define servers
SERVERS="cache1:9001,cache2:9002,cache3:9003"

# Auto-detect mode (cluster if topology service available, simple otherwise)
ocachecli --addr "$SERVERS" put "user:123" '{"name":"Alice"}'

# Force simple mode for basic distribution
ocachecli --mode simple --addr "$SERVERS" get "user:123"

# Force cluster mode for smart routing (requires topology service)
ocachecli --mode cluster --addr "$SERVERS" del "user:123"
```

### Performance Testing

See [Benchmark Guide](benchmark.md) for more details.

## Error Messages

The CLI provides clear error messages for common issues:

```bash
# Connection errors
Failed to create client: failed to create pool for localhost:9000

# Cluster mode specific
Failed to fetch initial topology: no topology service available
Error: cluster commands require cluster mode. Connected in simple mode.

# General errors
Get failed: rpc error: code = NotFound desc = key not found
```

## Best Practices

1. **Use Auto Mode**: Let the client detect the best mode automatically
2. **Pool Size Tuning**: Start with defaults, increase for high concurrency workloads
3. **Streaming**: Automatically used for values > 4MB
4. **Mode Selection**:
   - Use `simple` mode for development or standalone servers
   - Use `cluster` mode for production clusters with coordinator
   - Use `auto` mode when unsure (recommended)
5. **Benchmark First**: Test with your actual workload patterns

## Troubleshooting

### Connection Issues

```bash
# Test basic connectivity
ocachecli --addr localhost:9000 put test "value"

# Check if cluster mode is available
ocachecli --addr "node1:9001,node2:9002" cluster topology
# Will show error if cluster mode is not available

# Force specific mode to isolate issues
ocachecli --mode simple --addr "node1:9001" put test "value"
ocachecli --mode cluster --addr "node1:9001" put test "value"
```

### Mode Detection Issues

If auto mode isn't detecting correctly:

1. Check if coordinator service is running (for cluster mode)
2. Verify network connectivity to all nodes
3. Force the desired mode explicitly
4. Check server logs for topology service errors

## Exit Codes

- `0`: Success
- `1`: Error (invalid arguments, connection failure, operation failure, or interrupted)

## Summary

The CLI provides a simple yet powerful interface for interacting with OCache:

- **Auto mode** by default for zero configuration
- **Connection pooling** always enabled for better performance
- **Smart routing** in cluster mode for optimal key distribution
- **Simple mode** for straightforward multi-server setups
- **Consistent interface** regardless of deployment topology
