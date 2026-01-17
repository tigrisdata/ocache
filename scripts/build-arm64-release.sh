#!/bin/bash
#
# Build script for ARM64 Linux releases using QEMU emulation.
# This script runs inside an arm64v8/ubuntu:24.04 Docker container.
#

set -e

echo "=== OCache ARM64 Release Build ==="
echo "GO_VERSION: ${GO_VERSION}"
echo "ROCKSDB_VERSION: ${ROCKSDB_VERSION}"
echo "GITHUB_REPOSITORY: ${GITHUB_REPOSITORY}"

# Install system dependencies
echo "=== Installing system dependencies ==="
apt-get update
apt-get install -y \
    build-essential cmake curl git ca-certificates \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev \
    protobuf-compiler

# Install Go
echo "=== Installing Go ${GO_VERSION} ==="
curl -LO "https://go.dev/dl/go${GO_VERSION}.linux-arm64.tar.gz"
tar -C /usr/local -xzf "go${GO_VERSION}.linux-arm64.tar.gz"
rm "go${GO_VERSION}.linux-arm64.tar.gz"
export PATH="/usr/local/go/bin:$PATH"
export GOPATH="/go"
export PATH="$GOPATH/bin:$PATH"
go version

# Download or build RocksDB static library
echo "=== Setting up RocksDB ==="
ARTIFACT_NAME="rocksdb-static-${ROCKSDB_VERSION}-linux-aarch64.tar.gz"
RELEASE_URL="https://github.com/${GITHUB_REPOSITORY}/releases/download/rocksdb-v${ROCKSDB_VERSION}/${ARTIFACT_NAME}"

mkdir -p rocksdb-static/artifact

if curl -fsSL -o "${ARTIFACT_NAME}" "${RELEASE_URL}" 2>/dev/null; then
    echo "Downloaded pre-built RocksDB artifact from release"
    tar xzf "${ARTIFACT_NAME}" -C rocksdb-static/artifact
    rm "${ARTIFACT_NAME}"
else
    echo "Pre-built RocksDB artifact not found, building from source..."
    echo "This may take 30-60 minutes under QEMU emulation"
    ROCKSDB_VERSION=${ROCKSDB_VERSION} OS=linux ARCH=aarch64 ./scripts/build-rocksdb-static.sh
    tar xzf rocksdb-static/rocksdb-static-*.tar.gz -C rocksdb-static/artifact
fi

ls -la rocksdb-static/artifact/

# Install protoc plugins
echo "=== Installing protoc plugins ==="
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest

# Generate proto files
echo "=== Generating proto files ==="
make proto

# Build binaries with static RocksDB
echo "=== Building binaries ==="
GOOS=linux GOARCH=arm64 STATIC_BUILD=true make all

# Stage artifacts in dist directory
echo "=== Staging artifacts ==="
mkdir -p dist
mv ocache dist/ocache-linux-arm64
mv ocachecli dist/ocachecli-linux-arm64

ls -la dist/

echo "=== ARM64 release build completed successfully! ==="
