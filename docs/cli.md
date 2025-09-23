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
| `--pool-size`        | Connection pool size per address                               | `4`              |
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

## Mode Detection and Behavior

### Auto Mode Behavior

When using the default `auto` mode:

1. The client attempts to connect to the provided addresses
2. It checks if a cluster topology service is available
3. If topology service is found → operates in cluster mode
4. If no topology service → operates in simple mode
5. Displays the detected mode when starting (e.g., "Using cluster mode" or "Using simple mode")

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

All modes use connection pooling for better performance:

- **Default pool size**: 4 connections per address
- **Configurable**: Use `--pool-size` to adjust
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

# High-performance benchmark with larger pool
ocachecli \
  --addr "$SERVERS" \
  --pool-size 10 \
  bench \
  --num-keys 100000 \
  --num-ops 1000000 \
  --concurrency 64 \
  --value-size 4096 \
  --workload "read=80,update=20"
```

### Performance Testing

```bash
# Test with auto mode (single server will use simple mode)
ocachecli bench \
  --num-keys 5000 \
  --value-size 1024 \
  --num-ops 50000 \
  --concurrency 16

# Test with multiple servers (auto-detects appropriate mode)
ocachecli \
  --addr "node1:9001,node2:9002,node3:9003" \
  bench \
  --num-keys 5000 \
  --value-size 1024 \
  --num-ops 50000 \
  --concurrency 16

# Force cluster mode for comparison
ocachecli \
  --mode cluster \
  --addr "node1:9001,node2:9002,node3:9003" \
  --pool-size 8 \
  bench \
  --num-keys 5000 \
  --value-size 1024 \
  --num-ops 50000 \
  --concurrency 16
```

## Performance Tuning

### Pool Size Guidelines

Adjust `--pool-size` based on your workload:

- **Low concurrency** (< 10 concurrent operations): `--pool-size 2-3`
- **Medium concurrency** (10-50 concurrent operations): `--pool-size 4-8`
- **High concurrency** (50+ concurrent operations): `--pool-size 8-16`

Formula: `pool_size = min(expected_concurrency / 2, 16)`

### Topology Refresh Interval (Cluster Mode)

- **Stable cluster**: `--topology-refresh 60s`
- **Dynamic cluster**: `--topology-refresh 10s`
- **Development/testing**: `--topology-refresh 5s`

## Error Messages

The CLI provides clear error messages for common issues:

```bash
# Connection errors
Failed to create client: failed to create pool for localhost:9000

# Mode detection
Using simple mode  # When no topology service is found
Using cluster mode # When topology service is available

# Cluster mode specific
Failed to fetch initial topology: no topology service available

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

# Check mode detection
ocachecli --addr "node1:9001,node2:9002" put test "value"
# Look for "Using simple mode" or "Using cluster mode" in output

# Force specific mode to isolate issues
ocachecli --mode simple --addr "node1:9001" put test "value"
ocachecli --mode cluster --addr "node1:9001" put test "value"
```

### Performance Issues

```bash
# Increase pool size for better concurrency
ocachecli --pool-size 10 bench --concurrency 50

# For cluster mode, adjust topology refresh
ocachecli --mode cluster --topology-refresh 60s bench

# Compare modes to identify bottlenecks
ocachecli --mode simple bench --concurrency 20
ocachecli --mode cluster bench --concurrency 20
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

## Advanced Usage

### Scripting

The CLI is designed to be scriptable:

```bash
#!/bin/bash
# backup.sh - Backup all user data

CACHE_SERVERS="cache1:9001,cache2:9002"

# List all user keys
keys=$(ocachecli --addr "$CACHE_SERVERS" list --prefix "user:")

# Backup each key
for key in $keys; do
  value=$(ocachecli --addr "$CACHE_SERVERS" get "$key")
  echo "$key=$value" >> backup.txt
done
```

## Summary

The unified CLI provides a simple yet powerful interface for interacting with OCache:

- **Auto mode** by default for zero configuration
- **Connection pooling** always enabled for better performance
- **Smart routing** in cluster mode for optimal key distribution
- **Simple mode** for straightforward multi-server setups
- **Consistent interface** regardless of deployment topology
