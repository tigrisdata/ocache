#!/bin/bash

set -e

ROCKSDB_VERSION="${ROCKSDB_VERSION:-9.6.1}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"

echo "Installing RocksDB ${ROCKSDB_VERSION} to ${INSTALL_PREFIX}"

# Install build dependencies
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if command -v apt-get &> /dev/null; then
        echo "Installing build dependencies on Debian/Ubuntu..."
        sudo apt-get update
        sudo apt-get install -y build-essential cmake libgflags-dev libsnappy-dev \
            liblz4-dev libzstd-dev zlib1g-dev libbz2-dev
    elif command -v yum &> /dev/null; then
        echo "Installing build dependencies on RHEL/CentOS..."
        sudo yum install -y gcc-c++ cmake gflags-devel snappy-devel \
            lz4-devel libzstd-devel zlib-devel bzip2-devel
    elif command -v dnf &> /dev/null; then
        echo "Installing build dependencies on Fedora..."
        sudo dnf install -y gcc-c++ cmake gflags-devel snappy-devel \
            lz4-devel libzstd-devel zlib-devel bzip2-devel
    else
        echo "Unsupported Linux distribution"
        exit 1
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Installing build dependencies on macOS..."
    brew install cmake gflags snappy lz4 zstd
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi

# Create temporary build directory
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

# Download and extract RocksDB
echo "Downloading RocksDB ${ROCKSDB_VERSION}..."
curl -L -o rocksdb.tar.gz "https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz"
tar xzf rocksdb.tar.gz
cd "rocksdb-${ROCKSDB_VERSION}"

# Build RocksDB
echo "Building RocksDB..."
mkdir build
cd build
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DROCKSDB_BUILD_SHARED=ON \
    -DWITH_GFLAGS=ON \
    -DWITH_SNAPPY=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZ2=ON \
    -DUSE_RTTI=ON \
    -DPORTABLE=ON

make -j${JOBS}

# Install RocksDB
echo "Installing RocksDB..."
if [[ "${INSTALL_PREFIX}" == "/usr/local" ]]; then
    sudo make install
else
    make install
fi

# Update library cache on Linux
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo ldconfig
fi

# Clean up
cd /
rm -rf "$TMPDIR"

echo "RocksDB ${ROCKSDB_VERSION} installed successfully to ${INSTALL_PREFIX}"
echo ""
echo "To use this installation, set the following environment variables:"
echo "export CGO_CFLAGS=\"-I${INSTALL_PREFIX}/include\""
echo "export CGO_LDFLAGS=\"-L${INSTALL_PREFIX}/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd\""