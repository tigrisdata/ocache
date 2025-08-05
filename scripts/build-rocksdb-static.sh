#!/bin/bash

set -e

ROCKSDB_VERSION="${ROCKSDB_VERSION:-10.4.2}"
BUILD_DIR="${BUILD_DIR:-$(pwd)/rocksdb-static}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
ARCH="${ARCH:-$(uname -m)}"
# Get OS name in lowercase
OS_RAW="$(uname -s)"
case "${OS_RAW}" in
    Linux*) OS_COMPUTED="linux" ;;
    Darwin*) OS_COMPUTED="darwin" ;;
    *) OS_COMPUTED="${OS_RAW,,}" ;;
esac
OS="${OS:-${OS_COMPUTED}}"

echo "Building static RocksDB ${ROCKSDB_VERSION} for ${OS}-${ARCH}"
echo "Build directory: ${BUILD_DIR}"

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Download and extract RocksDB
if [ ! -f "rocksdb-${ROCKSDB_VERSION}.tar.gz" ]; then
    echo "Downloading RocksDB ${ROCKSDB_VERSION}..."
    curl -L -o "rocksdb-${ROCKSDB_VERSION}.tar.gz" "https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz"
fi

if [ ! -d "rocksdb-${ROCKSDB_VERSION}" ]; then
    echo "Extracting RocksDB..."
    tar xzf "rocksdb-${ROCKSDB_VERSION}.tar.gz"
fi

cd "rocksdb-${ROCKSDB_VERSION}"

# Clean previous builds
rm -rf build
mkdir build
cd build

# Configure for static build
echo "Configuring RocksDB for static build..."
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DROCKSDB_BUILD_SHARED=OFF \
    -DWITH_GFLAGS=OFF \
    -DWITH_SNAPPY=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZ2=ON \
    -DUSE_RTTI=ON \
    -DPORTABLE=ON \
    -DWITH_TESTS=OFF \
    -DWITH_TOOLS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON

# Build static library
echo "Building RocksDB static library..."
make -j${JOBS} rocksdb

# Create artifact directory structure
ARTIFACT_DIR="${BUILD_DIR}/artifact"
mkdir -p "${ARTIFACT_DIR}/lib"
mkdir -p "${ARTIFACT_DIR}/include"

# Copy built artifacts
echo "Copying artifacts..."
cp librocksdb.a "${ARTIFACT_DIR}/lib/"
cp -r ../include/rocksdb "${ARTIFACT_DIR}/include/"

# Create metadata file
cat > "${ARTIFACT_DIR}/metadata.json" << EOF
{
    "version": "${ROCKSDB_VERSION}",
    "os": "${OS}",
    "arch": "${ARCH}",
    "build_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "static": true
}
EOF

# Create tarball
cd "${BUILD_DIR}"
TAR_NAME="rocksdb-static-${ROCKSDB_VERSION}-${OS}-${ARCH}.tar.gz"
echo "Creating artifact tarball: ${TAR_NAME}"
tar czf "${TAR_NAME}" -C artifact .

echo "Static RocksDB build complete!"
echo "Artifact: ${BUILD_DIR}/${TAR_NAME}"
echo ""
echo "To use this static build:"
echo "1. Extract the tarball to your desired location"
echo "2. Set CGO_CFLAGS=\"-I/path/to/extracted/include\""
echo "3. Set CGO_LDFLAGS=\"-L/path/to/extracted/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread\""