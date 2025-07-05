# Cache Service

A cache service that supports both in-memory and disk-based storage, with gRPC and HTTP interfaces. It uses a combination of RocksDB and local storage to manage cache items efficiently, and provides fast access to both small and large objects. The service is designed to handle high throughput and low latency, making it suitable for various caching scenarios.

## Prerequisites

- Go (1.18 or newer recommended)
- [Homebrew](https://brew.sh/) (for installing dependencies on macOS)
- [RocksDB](https://github.com/facebook/rocksdb)

## Installation

### Clone the repository

```bash
git clone <repo-url>
cd ocache
```

### Build the service (macOS)

Install RocksDB:

```bash
brew install rocksdb
```

Generate the Go code for gRPC, at the root of the repo:

```bash
protoc -I ./proto \
  --go_out=paths=source_relative:./proto \
  --go-grpc_out=paths=source_relative:./proto \
  proto/cache.proto
```

Then build the service:

```bash
CGO_CFLAGS="-I$(brew --prefix)/include" CGO_LDFLAGS="-L$(brew --prefix)/lib" go build -o ocache ./server/
```

## Usage

Run the service:

```bash
./ocache
```

### Command-line Flags

- `-disk` : Path for disk spill (default: `/var/cache`)
- `-threshold`: Small object threshold in bytes (default: 262144)
- `-ttl`: Time-to-live for items in seconds (default: 900)
- `-port` : gRPC listen port (default: 9000)
- `-http-port` : HTTP listen port (default: 9001)
- `-v` : Enable debug logging (default: no)

Example:

```bash
./ocache -disk /tmp/mydisk -threshold 2097152 -port 9090 -http-port 9091 -ttl 3600 -v
```

## HTTP Endpoints

- `POST  /v1/cache/{key}` : Store an item (body: JSON with `data` as base64, or use a tool that sends binary data)
- `GET   /v1/cache/{key}` : Retrieve an item
- `DELETE /v1/cache/{key}` : Delete an item
- `GET   /v1/cache` : List items

### Examples

**Store an item (base64-encoded data):**

```bash
curl -X POST "http://localhost:9001/v1/cache/mykey" \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","ttl_seconds":3600,"data":"aGVsbG8gd29ybGQ="}'
```

**Store an item (raw binary data, if supported):**

```bash
curl -X POST "http://localhost:9001/v1/cache/mykey" \
  --data-binary @myfile.bin
```

**Retrieve an item:**

```bash
curl "http://localhost:9001/v1/cache/mykey"
```

**Delete an item:**

```bash
curl -X DELETE "http://localhost:9001/v1/cache/mykey"
```

**List all items:**

```bash
curl "http://localhost:9001/v1/cache"
```

## CLI Usage

A command-line client is available for interacting with the cache service via gRPC.

### Build the CLI

```bash
go build -o ocachecli ./client/cmd/
```

### CLI Commands

- `put <key> <value>`: Store a value in the cache
- `get <key>`: Retrieve a value from the cache
- `del <key>`: Delete a key from the cache
- `list`: List all keys in the cache
- `bench`: Run a benchmark test

You can specify the server address with `--addr` (default: `localhost:9000`).

### Examples

**Store a value:**

```bash
./ocachecli --addr localhost:9000 put mykey "hello world"
```

**Retrieve a value:**

```bash
./ocachecli --addr localhost:9000 get mykey
```

**Delete a key:**

```bash
./ocachecli --addr localhost:9000 del mykey
```

**List all keys:**

```bash
./ocachecli --addr localhost:9000 list
```

For more help, run:

```bash
./ocachecli --help
```

### Benchmarks

To run benchmarks, use the `bench` command in the CLI:

```bash
./ocachecli --addr localhost:9000 bench
```

This will run a YCSB style benchmark against the cache service, simulating a workload with configurable parameters.

#### Benchmark Options

- `--concurrency`: Number of concurrent workers (default 8)
- `--num-keys`: Number of unique keys (default 1000)
- `--num-ops`: Total number of operations (default 10000)
- `--value-size`: Value size in bytes (default 100)
- `--workload`: Workload type or custom mix (e.g. A, B, read=70,update=30) (default "A")
