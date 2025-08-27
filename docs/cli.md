# CLI Client Documentation

The OCache CLI client (`ocachecli`) provides a command-line interface for interacting with the cache service via gRPC.

## Installation

### Using Makefile

```bash
make build-cli
```

### Building using go build

```bash
go build -o ocachecli ./client/cmd/
```

## Basic Usage

```bash
ocachecli [global options] command [command options] [arguments...]
```

### Global Options

- `--addr value`: Server address (default: `localhost:9000`)
- `--help, -h`: Show help message
- `--version, -v`: Show version information

### Commands

#### put - Store a value

```bash
ocachecli put <key> <value> [options]
```

**Options:**

- `--ttl`: Time-to-live in seconds

**Examples:**

```bash
# Store simple string
ocachecli put mykey "hello world"

# Store with TTL
ocachecli put session "data" --ttl 3600

# Store from stdin
echo "piped data" | ocachecli put pipekey
```

#### get - Retrieve a value

```bash
ocachecli get <key> [options]
```

**Examples:**

```bash
# Get and display value
ocachecli get mykey

# Get and pipe to another command
ocachecli get data | jq .
```

#### delete - Remove a key

```bash
ocachecli del <key>
```

**Examples:**

```bash
# Delete a key
ocachecli del old-data

# Delete multiple keys
for key in key1 key2 key3; do
  ocachecli del $key
done
```

#### list - List keys

```bash
ocachecli list [options]
```

**Options:**

- `--prefix`: Filter by prefix

**Examples:**

```bash
# List all keys
ocachecli list

# List with prefix
ocachecli list --prefix user:

# Count keys
ocachecli list | wc -l
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

## Troubleshooting

### Connection Issues

```bash
# Test connectivity
nc -zv localhost 9000

# Check server logs
./ocache -v

# Try with explicit IPv4
ocachecli --addr 127.0.0.1:9000 list
```

### Performance Issues

```bash
# Run baseline benchmark
ocachecli bench --num-ops 1000

# Test with different concurrency
ocachecli bench --concurrency 1
ocachecli bench --concurrency 32

# Test with different value sizes
ocachecli bench --value-size 10
ocachecli bench --value-size 100000
```

## See Also

- [Client Documentation](client.md) - Complete client documentation including Go library
- [HTTP API Reference](http_api.md) - HTTP REST API documentation
- [Configuration](configuration.md) - Server configuration options
