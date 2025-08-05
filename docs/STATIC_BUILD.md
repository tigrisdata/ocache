# Static Build with RocksDB

This document explains how to build OCache with a statically linked RocksDB library for improved portability and faster CI/CD workflows.

## Benefits

1. **Faster CI/CD**: RocksDB is built once and cached, eliminating the need to rebuild on every workflow run
2. **Portability**: Single binary with RocksDB statically linked
3. **Consistency**: Same RocksDB version across all builds
4. **Simplified deployment**: No need to install RocksDB on target systems

## Platform Notes

### Linux
On Linux, the build creates a fully static binary with all dependencies included.

### macOS
On macOS, the build creates a mostly-static binary where RocksDB is statically linked, but system libraries (like libc) remain dynamically linked. This is because macOS doesn't support fully static binaries.

## Building Locally

### Quick Start

Build OCache with static RocksDB:

```bash
make build-static
```

### Manual Build Process

1. Build RocksDB static library:
   ```bash
   make build-rocksdb-static
   ```

2. Build OCache with static linking:
   ```bash
   STATIC_BUILD=true make all
   ```

### Custom RocksDB Version

```bash
ROCKSDB_VERSION=10.4.2 make build-rocksdb-static
```

## CI/CD Workflow

The GitHub Actions workflow automatically:

1. Checks for cached RocksDB artifacts
2. Downloads pre-built artifacts from GitHub releases (if available)
3. Falls back to building from source if needed
4. Caches the artifact for future runs

## Pre-built Artifacts

Pre-built RocksDB static libraries are available as GitHub releases for:

- Linux x86_64
- Linux aarch64
- macOS x86_64
- macOS arm64

### Using Pre-built Artifacts

1. Download the appropriate artifact from the releases page
2. Extract to your project:
   ```bash
   tar xzf rocksdb-static-10.4.2-linux-x86_64.tar.gz -C rocksdb-static/artifact
   ```
3. Build with static linking:
   ```bash
   STATIC_BUILD=true make all
   ```

## Build Configuration

The static build uses the following RocksDB configuration:

- Compression: Snappy, LZ4, Zstd, Zlib, BZ2
- Position Independent Code (PIC): Enabled
- RTTI: Enabled
- Portable: Enabled
- Tests/Tools: Disabled (for smaller artifact size)

## Troubleshooting

### Missing Dependencies

On Linux, ensure you have the required compression libraries:

```bash
sudo apt-get install -y libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev
```

On macOS:

```bash
brew install snappy lz4 zstd
```

### Build Failures

If the static build fails, check:

1. Available disk space (RocksDB build requires ~2GB)
2. Installed dependencies
3. CMake version (3.10+ required)

### Runtime Issues

If you encounter runtime errors with the static binary:

1. Ensure all compression libraries are statically linked
2. Check for missing system libraries:
   - Linux: `ldd ocache`
   - macOS: `otool -L ocache`
3. Verify the binary was built with `STATIC_BUILD=true`

### macOS Specific Issues

If you see errors about `-static` on macOS:
- This is expected as macOS doesn't support fully static binaries
- The build automatically uses appropriate flags for macOS
- The resulting binary will have RocksDB statically linked but system libraries dynamically linked