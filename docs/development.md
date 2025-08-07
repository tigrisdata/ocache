# Development Guide

## Project Structure

```
ocache/
├── server/          # Server implementation
│   └── main.go      # Server entry point
├── client/          # Client library and CLI
│   ├── cmd/         # CLI implementation
│   └── client.go    # Client library
├── internal/        # Internal packages
│   ├── cache/       # Core cache implementation
│   ├── storage/     # Storage backends
│   └── cleaner/     # TTL cleanup
├── proto/           # Protocol buffer definitions
│   └── cache.proto  # gRPC service definition
├── docs/            # Documentation
└── Makefile         # Build automation
```

## Development Setup

### Prerequisites

- Go 1.19 or higher
- Protocol Buffers compiler (protoc)
- RocksDB development libraries
- Make (for using Makefile)

### Setting Up Development Environment

1. Clone the repository:
```bash
git clone https://github.com/tigrisdata/ocache.git
cd ocache
```

2. Install dependencies:
```bash
make install-deps
```

3. Generate gRPC code:
```bash
make proto
```

4. Build the project:
```bash
make all
```

## Testing

### Running Tests

Run all tests:
```bash
make test
```

Run tests with coverage:
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

Run specific package tests:
```bash
go test ./internal/cache
go test ./internal/storage
```

### Running Benchmarks

Run all benchmarks:
```bash
make bench
```

Run specific benchmarks:
```bash
go test -bench=. ./internal/cache
```

Run the end-to-end benchmark:
```bash
./ocachecli --addr localhost:9000 bench
```

## Code Organization

### Core Components

#### Cache Interface (`internal/cache`)
- Defines the main cache operations
- Handles TTL management
- Coordinates between storage backends

#### Storage Layer (`internal/storage`)
- **RocksDB Storage**: For small objects and metadata
- **File Storage**: For large objects
- **Segment Management**: Handles file segmentation

#### Cleaner (`internal/cleaner`)
- Background process for TTL expiration
- Periodic cleanup of expired items
- Disk space management

### Adding New Features

1. **New Storage Backend**:
   - Implement the storage interface
   - Add configuration flags
   - Update cache initialization

2. **New API Endpoint**:
   - Update `proto/cache.proto`
   - Regenerate gRPC code
   - Implement server handler
   - Add HTTP endpoint if needed

3. **New CLI Command**:
   - Add command to `client/cmd/`
   - Update help text
   - Add tests

## Building and Running

### Development Build

Quick build for testing:
```bash
go build -o ocache ./server/
go build -o ocachecli ./client/cmd/
```

### Production Build

Static build with optimizations:
```bash
make build-static
```

### Running with Debug Logging

```bash
./ocache -v -disk /tmp/test-cache
```

### Running Tests During Development

Watch mode (requires external tool):
```bash
watch -n 2 'go test ./...'
```

## Debugging

### Enable Verbose Logging

Run the server with `-v` flag:
```bash
./ocache -v
```

### Using Delve Debugger

Install Delve:
```bash
go install github.com/go-delve/delve/cmd/dlv@latest
```

Debug the server:
```bash
dlv debug ./server -- -v -disk /tmp/debug-cache
```

### Performance Profiling

Enable CPU profiling:
```bash
go run ./server -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

Enable memory profiling:
```bash
go run ./server -memprofile=mem.prof
go tool pprof mem.prof
```

## Contributing

### Code Style

- Follow standard Go conventions
- Use `gofmt` for formatting
- Use `golint` for linting
- Keep functions small and focused
- Add comments for exported functions

### Testing Requirements

- Write tests for new features
- Maintain test coverage above 70%
- Include benchmarks for performance-critical code
- Test error cases and edge conditions

### Commit Guidelines

- Use clear, descriptive commit messages
- Reference issues in commit messages
- Keep commits focused and atomic
- Run tests before committing

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Update documentation
6. Submit a pull request

## Common Development Tasks

### Regenerating Protocol Buffers

After modifying `proto/cache.proto`:
```bash
make proto
```

### Updating Dependencies

```bash
go mod tidy
go mod vendor
```

### Running Linters

```bash
golangci-lint run
```

### Checking for Race Conditions

```bash
go test -race ./...
```

## Architecture Decisions

### Why RocksDB?

- Embedded key-value store
- Excellent performance
- Built-in compression
- Efficient for small objects

### Why Dual Storage?

- Optimize for both small and large objects
- Reduce memory usage for large files
- Maintain fast access for small items

### Why gRPC and HTTP?

- gRPC for high-performance internal communication
- HTTP for easy integration and debugging
- Support for different client types

## Troubleshooting

### Build Issues

**RocksDB not found:**
- Ensure RocksDB is installed
- Check CGO flags in build command
- Consider using static build

**Protocol buffer errors:**
- Update protoc compiler
- Regenerate proto files
- Check import paths

### Runtime Issues

**High memory usage:**
- Adjust threshold settings
- Check for memory leaks with pprof
- Monitor cache size

**Slow performance:**
- Enable profiling
- Check disk I/O
- Tune segment size
- Adjust FD cache size

## Future Improvements

- [ ] Distributed caching support
- [ ] Metrics and monitoring endpoints
- [ ] Configuration file support
- [ ] Docker image
- [ ] Kubernetes operator
- [ ] Additional storage backends (S3, etc.)
- [ ] Cache warming/preloading
- [ ] Advanced eviction policies