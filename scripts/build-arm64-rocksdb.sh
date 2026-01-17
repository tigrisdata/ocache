#!/bin/bash
#
# Build script for ARM64 RocksDB static library using QEMU emulation.
# This script runs inside an arm64v8/ubuntu:24.04 Docker container.
#

set -e

echo "=== RocksDB ARM64 Build ==="
echo "ROCKSDB_VERSION: ${ROCKSDB_VERSION}"

# Install system dependencies
echo "=== Installing system dependencies ==="
apt-get update
apt-get install -y \
    build-essential cmake curl git ca-certificates \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev

# Build RocksDB
echo "=== Building RocksDB static library ==="
echo "This may take 30-60 minutes under QEMU emulation"
ROCKSDB_VERSION=${ROCKSDB_VERSION} OS=linux ARCH=aarch64 ./scripts/build-rocksdb-static.sh

ls -la rocksdb-static/

echo "=== RocksDB ARM64 build completed successfully! ==="
