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
