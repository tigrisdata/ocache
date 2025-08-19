# CLI Client Documentation

The OCache CLI client (`ocachecli`) provides a command-line interface for interacting with the cache service via gRPC.

## Installation

### Building from Source

```bash
go build -o ocachecli ./client/cmd/
```

### Using Makefile

```bash
make cli
```

## Basic Usage

```bash
./ocachecli [global options] command [command options] [arguments...]
```

### Global Options

- `--addr value`: Server address (default: `localhost:9000`)
- `--help, -h`: Show help message
- `--version, -v`: Show version information

## Commands

### put - Store a value

Store a key-value pair in the cache.

```bash
./ocachecli put <key> <value>
```

**Examples:**

```bash
# Store a simple string
./ocachecli put mykey "hello world"

# Store with custom server address
./ocachecli --addr cache.example.com:9000 put config "{'timeout': 30}"

# Store JSON data
./ocachecli put user:123 '{"name": "John", "age": 30}'
```

### get - Retrieve a value

Retrieve a value by its key.

```bash
./ocachecli get <key>
```

**Examples:**

```bash
# Get a value
./ocachecli get mykey

# Get from specific server
./ocachecli --addr cache.example.com:9000 get mykey
```

**Output:**
- Returns the stored value if found
- Returns error message if key doesn't exist

### del - Delete a key

Remove a key-value pair from the cache.

```bash
./ocachecli del <key>
```

**Examples:**

```bash
# Delete a key
./ocachecli del mykey

# Delete from specific server
./ocachecli --addr cache.example.com:9000 del old-data
```

### list - List all keys

List all keys currently stored in the cache.

```bash
./ocachecli list
```

**Examples:**

```bash
# List all keys
./ocachecli list

# List from specific server
./ocachecli --addr cache.example.com:9000 list
```

**Output:**
- Returns a list of all cache keys
- Empty list if no keys exist

### bench - Run benchmarks

Run performance benchmarks against the cache service.

```bash
./ocachecli bench [options]
```

**Options:**

- `--concurrency value`: Number of concurrent workers (default: 8)
- `--num-keys value`: Number of unique keys (default: 1000)
- `--num-ops value`: Total number of operations (default: 10000)
- `--value-size value`: Value size in bytes (default: 100)
- `--workload value`: Workload type or custom mix (default: "A")

**Workload Types:**

| Type | Description | Read % | Update % | Insert % | RMW % |
|------|-------------|--------|----------|----------|-------|
| A | Update heavy | 50 | 50 | 0 | 0 |
| B | Read mostly | 95 | 5 | 0 | 0 |
| C | Read only | 100 | 0 | 0 | 0 |
| D | Read latest | 95 | 0 | 5 | 0 |
| F | Read-modify-write | 50 | 0 | 0 | 50 |
| Custom | User defined | Specify as `read=70,update=30` |

**Examples:**

```bash
# Run default benchmark (Workload A)
./ocachecli bench

# Run read-heavy benchmark with more operations
./ocachecli bench --workload B --num-ops 100000

# Run with custom workload mix
./ocachecli bench --workload "read=70,update=20,insert=10"

# High concurrency test
./ocachecli bench --concurrency 50 --num-keys 10000

# Large value test
./ocachecli bench --value-size 10240 --num-ops 1000

# Comprehensive benchmark
./ocachecli bench \
  --workload B \
  --concurrency 16 \
  --num-keys 5000 \
  --num-ops 50000 \
  --value-size 1024
```

**Benchmark Examples by Object Size:**

```bash
# Small objects (100 bytes) - Tests RocksDB inline storage
# Good for testing metadata, cache keys, session tokens
./ocachecli bench \
  --value-size 100 \
  --num-keys 10000 \
  --num-ops 100000 \
  --workload A \
  --concurrency 16

# Medium objects (100 KB) - Tests file-based storage with compaction
# Good for testing user profiles, API responses, small images
./ocachecli bench \
  --value-size 102400 \
  --num-keys 1000 \
  --num-ops 10000 \
  --workload B \
  --concurrency 8

# Large objects (1 MB) - Tests raw file storage without compaction
# Good for testing documents, images, large datasets
./ocachecli bench \
  --value-size 1048576 \
  --num-keys 100 \
  --num-ops 1000 \
  --workload C \
  --concurrency 4

# Mixed workload benchmark suite
# Run all three sizes sequentially to test cache behavior across object sizes
echo "=== Testing 100 byte objects (RocksDB) ==="
./ocachecli bench --value-size 100 --num-keys 10000 --num-ops 50000 --workload A

echo "=== Testing 100 KB objects (File storage with compaction) ==="
./ocachecli bench --value-size 102400 --num-keys 500 --num-ops 5000 --workload B

echo "=== Testing 1 MB objects (Raw file storage) ==="
./ocachecli bench --value-size 1048576 --num-keys 50 --num-ops 500 --workload C
```

**Storage Strategy by Size:**
- **< 64 KB**: Stored inline in RocksDB for fast access
- **64 KB - 16 MB**: Initially stored as files, eligible for segment compaction
- **> 16 MB**: Permanent raw file storage, never compacted

**Performance Tuning Examples:**

```bash
# Test cache performance under memory pressure
# Small objects with high concurrency
./ocachecli bench \
  --value-size 100 \
  --num-keys 100000 \
  --num-ops 1000000 \
  --workload "read=80,update=20" \
  --concurrency 32

# Test disk I/O performance
# Large objects with sequential access
./ocachecli bench \
  --value-size 1048576 \
  --num-keys 100 \
  --num-ops 1000 \
  --workload D \
  --concurrency 2

# Test compaction behavior
# Medium objects with mixed read/write
./ocachecli bench \
  --value-size 102400 \
  --num-keys 2000 \
  --num-ops 20000 \
  --workload F \
  --concurrency 8
```

**Output:**
The benchmark command provides detailed statistics including:
- Total operations completed
- Operations per second (throughput)
- Latency percentiles (p50, p95, p99)
- Operation breakdown by type
- Error count (if any)

## Advanced Usage

### Connecting to Remote Servers

```bash
# Connect to production cache
./ocachecli --addr prod-cache.example.com:9000 get important-data

# Connect to staging environment
./ocachecli --addr staging-cache.example.com:9000 list
```

### Scripting Examples

#### Bulk Load Data

```bash
#!/bin/bash
# Load data from a file
while IFS='=' read -r key value; do
  ./ocachecli put "$key" "$value"
done < data.txt
```

#### Cache Warming

```bash
#!/bin/bash
# Warm cache with common keys
keys=("config" "user:prefs" "app:settings")
for key in "${keys[@]}"; do
  ./ocachecli get "$key" > /dev/null 2>&1
done
```

#### Monitor Cache Size

```bash
#!/bin/bash
# Count number of keys
count=$(./ocachecli list | wc -l)
echo "Cache contains $count keys"
```

## Error Handling

The CLI client returns appropriate exit codes:
- `0`: Success
- `1`: General error
- `2`: Connection error
- `3`: Invalid arguments

Common error messages:
- `connection refused`: Server is not running or wrong address
- `key not found`: Attempting to get a non-existent key
- `invalid argument`: Malformed command or options

## Performance Tips

1. **Batch Operations**: For bulk operations, consider using the gRPC API directly for better performance
2. **Connection Reuse**: The CLI creates a new connection for each operation; use the client library for persistent connections
3. **Benchmarking**: Use appropriate workload patterns that match your use case
4. **Value Size**: Test with realistic value sizes for accurate benchmarks

## Troubleshooting

### Connection Issues

```bash
# Test connectivity
nc -zv localhost 9000

# Check server logs
./ocache -v

# Try with explicit IPv4
./ocachecli --addr 127.0.0.1:9000 list
```

### Performance Issues

```bash
# Run baseline benchmark
./ocachecli bench --num-ops 1000

# Test with different concurrency
./ocachecli bench --concurrency 1
./ocachecli bench --concurrency 32

# Test with different value sizes
./ocachecli bench --value-size 10
./ocachecli bench --value-size 100000
```

## See Also

- [API Reference](api.md) - Detailed API documentation
- [Configuration](configuration.md) - Server configuration options
- [Development Guide](development.md) - Building and contributing