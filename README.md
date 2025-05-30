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
- `-threshold`: Small object threshold in bytes (default: 131072)
- `-ttl`: Time-to-live for items in seconds (default: 900)
- `-port` : HTTP listen port (default: 8080)

Example:

```bash
./cache_service -db mydb -disk /tmp/mydisk -threshold 2097152 -port 9090
```

## Endpoints

- `PUT   /put` : Store an item
- `GET   /get` : Retrieve an item
- `POST  /delete` : Delete an item
- `GET   /list` : List items

Refer to the code for request/response formats.
