# Installation

## Prerequisites

- Go 1.19+ (for building from source)
- RocksDB (for macOS/Linux builds)
- Protocol Buffers compiler (protoc) for regenerating gRPC code

## Clone the Repository

```bash
git clone https://github.com/tigrisdata/ocache.git
cd ocache
```

## Build Options

### Quick Start (with Makefile)

The easiest way to build OCache is using the provided Makefile:

```bash
# Install dependencies and build
make install-deps
make all
```

This will:

- Install required Go dependencies
- Generate gRPC code from protobuf definitions
- Build the server and CLI binaries

### Static Build (Recommended for Production)

For better portability and deployment, build with statically linked RocksDB:

```bash
# Build RocksDB static library (only needed once)
make build-rocksdb-static

# Build OCache with static linking
make build-static
```

The static build:

- Includes RocksDB library in the binary
- Reduces runtime dependencies
- Improves deployment portability

See [static_build.md](static_build.md) for detailed static build instructions.

### Manual Build

#### macOS

Install RocksDB via Homebrew:

```bash
brew install rocksdb
```

Generate gRPC code:

```bash
protoc -I ./proto \
  --go_out=paths=source_relative:./proto \
  --go-grpc_out=paths=source_relative:./proto \
  proto/cache.proto
```

Build the service:

```bash
CGO_CFLAGS="-I$(brew --prefix)/include" \
CGO_LDFLAGS="-L$(brew --prefix)/lib" \
go build -o ocache ./server/
```

#### Linux

Install RocksDB (Ubuntu/Debian):

```bash
sudo apt-get update
sudo apt-get install -y librocksdb-dev
```

Install RocksDB (RHEL/CentOS):

```bash
sudo yum install -y rocksdb-devel
```

Then follow the same steps as macOS for generating gRPC code and building.

## Build Targets

The Makefile provides several useful targets:

- `make all` - Build everything (server and CLI)
- `make server` - Build only the server
- `make cli` - Build only the CLI client
- `make proto` - Regenerate gRPC code from protobuf
- `make clean` - Clean build artifacts
- `make test` - Run tests
- `make bench` - Run benchmarks

## Verifying Installation

After building, verify the installation:

```bash
# Check the server binary
./ocache --help

# Check the CLI binary
./ocachecli --help
```

## Docker Build (Coming Soon)

Docker support is planned for easier deployment and containerized environments.
