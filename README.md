# cache_service

A simple HTTP cache service using RocksDB for storage.

## Prerequisites

- Go (1.18 or newer recommended)
- [Homebrew](https://brew.sh/) (for installing dependencies)
- [RocksDB](https://github.com/facebook/rocksdb)

## Installation (macOS)

## Install RocksDB

```bash
brew install rocksdb
```

### Clone the repository

```bash
git clone <your-repo-url>
cd cache_service
```

### Build the service

Generate the Go code for gRPC, at the root of the repo:

```bash
protoc -I ./proto \
  --go_out=paths=source_relative:./proto \
  --go-grpc_out=paths=source_relative:./proto \
  proto/cache.proto
```

Then build the service:

```bash
CGO_CFLAGS="-I$(brew --prefix)/include" CGO_LDFLAGS="-L$(brew --prefix)/lib" go build -o cache_service .
```

## Usage

Run the service:

```bash
./cache_service
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
./cache_service -disk /tmp/mydisk -threshold 2097152 -port 9090 -http-port 9091 -ttl 3600 -v
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
