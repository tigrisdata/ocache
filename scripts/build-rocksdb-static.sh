#!/bin/bash

set -e

ROCKSDB_VERSION="${ROCKSDB_VERSION:-10.4.2}"
BUILD_DIR="${BUILD_DIR:-$(pwd)/rocksdb-static}"
JOBS="${JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)}"
ARCH="${ARCH:-$(uname -m)}"

# Build variant:
#   plain    (default) - RocksDB only; artifact name unchanged for backward compat.
#   jemalloc           - RocksDB built WITH_JEMALLOC=ON, plus a statically-linked
#                        jemalloc (empty prefix, so it interposes the process
#                        malloc) bundled into the artifact. Linux only — the RSS
#                        problem this addresses is glibc-arena fragmentation, and
#                        fully-static linking (how the deployed binary consumes
#                        this) is a Linux concern. See issue #176.
VARIANT="${VARIANT:-plain}"
JEMALLOC_VERSION="${JEMALLOC_VERSION:-5.3.0}"

# Get OS name in lowercase
OS_RAW="$(uname -s)"
case "${OS_RAW}" in
    Linux*) OS_COMPUTED="linux" ;;
    Darwin*) OS_COMPUTED="darwin" ;;
    *) OS_COMPUTED="${OS_RAW,,}" ;;
esac
OS="${OS:-${OS_COMPUTED}}"

case "${VARIANT}" in
    plain)
        VARIANT_SUFFIX=""
        ;;
    jemalloc)
        VARIANT_SUFFIX="-jemalloc"
        # jemalloc bakes in a page-size assumption at build time and fully-static
        # glibc linking is Linux-only, so restrict this variant to Linux.
        case "${OS}" in
            Linux|linux) ;;
            *)
                echo "ERROR: VARIANT=jemalloc is only supported on Linux (OS=${OS})" >&2
                exit 1
                ;;
        esac
        ;;
    *)
        echo "ERROR: unknown VARIANT '${VARIANT}' (expected 'plain' or 'jemalloc')" >&2
        exit 1
        ;;
esac

echo "Building static RocksDB ${ROCKSDB_VERSION} (variant: ${VARIANT}) for ${OS}-${ARCH}"
echo "Build directory: ${BUILD_DIR}"

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# ---------------------------------------------------------------------------
# jemalloc (jemalloc variant only): build a static libjemalloc.a with an empty
# symbol prefix so it interposes the process malloc/free, and install it into a
# build-local prefix that RocksDB's cmake and the final artifact both consume.
# ---------------------------------------------------------------------------
JEMALLOC_PREFIX=""
if [ "${VARIANT}" = "jemalloc" ]; then
    JEMALLOC_PREFIX="${BUILD_DIR}/jemalloc-prefix"

    if [ ! -f "${JEMALLOC_PREFIX}/lib/libjemalloc.a" ]; then
        # The release tarball ships a pre-generated ./configure (the GitHub source
        # archive does not — it would need autoconf), so fetch the release asset.
        # This tarball's configure + generated Makefile are executed below, so its
        # integrity is verified against a pinned digest before extraction to keep
        # tampered upstream content out of the published artifact. If you bump
        # JEMALLOC_VERSION, update JEMALLOC_SHA256 to the new release's checksum.
        JEMALLOC_SHA256="${JEMALLOC_SHA256:-2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa}"
        JEMALLOC_TARBALL="jemalloc-${JEMALLOC_VERSION}.tar.bz2"
        if [ ! -f "${JEMALLOC_TARBALL}" ]; then
            echo "Downloading jemalloc ${JEMALLOC_VERSION}..."
            curl -fL -o "${JEMALLOC_TARBALL}" \
                "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/${JEMALLOC_TARBALL}"
        fi
        echo "Verifying jemalloc tarball checksum..."
        if command -v sha256sum >/dev/null 2>&1; then
            echo "${JEMALLOC_SHA256}  ${JEMALLOC_TARBALL}" | sha256sum -c -
        else
            echo "${JEMALLOC_SHA256}  ${JEMALLOC_TARBALL}" | shasum -a 256 -c -
        fi
        rm -rf "jemalloc-${JEMALLOC_VERSION}"
        tar xjf "${JEMALLOC_TARBALL}"

        (
            cd "jemalloc-${JEMALLOC_VERSION}"
            # --with-jemalloc-prefix= (empty): export plain malloc/free/... so
            #   jemalloc becomes the process allocator when linked, and so
            #   RocksDB's WITH_JEMALLOC integration (unprefixed on Linux) resolves.
            # --with-lg-page=12: pin 4 KiB pages. Our build hosts and deploy
            #   targets (x86-64 and Graviton/r7g on Ubuntu) are all 4 KiB; pinning
            #   avoids a page-size mismatch fault if ever built on a 64 KiB host.
            # static only, C-only (operator new/delete route through malloc, so C
            #   interposition is sufficient and avoids libstdc++ symbol clashes).
            ./configure \
                --prefix="${JEMALLOC_PREFIX}" \
                --with-jemalloc-prefix= \
                --with-lg-page=12 \
                --enable-static \
                --disable-shared \
                --disable-cxx
            make -j"${JOBS}"
            make install
        )
    fi
    echo "jemalloc static lib: ${JEMALLOC_PREFIX}/lib/libjemalloc.a"
fi

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
CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DROCKSDB_BUILD_SHARED=OFF
    -DWITH_GFLAGS=OFF
    -DWITH_SNAPPY=ON
    -DWITH_LZ4=ON
    -DWITH_ZSTD=ON
    -DWITH_ZLIB=ON
    -DWITH_BZ2=ON
    -DUSE_RTTI=ON
    -DPORTABLE=ON
    -DWITH_TESTS=OFF
    -DWITH_TOOLS=OFF
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)

if [ "${VARIANT}" = "jemalloc" ]; then
    # WITH_JEMALLOC lets RocksDB use jemalloc's nodump arenas for the block cache
    # and malloc_usable_size for accurate accounting. CMAKE_PREFIX_PATH points its
    # FindJeMalloc at our freshly-built static jemalloc.
    CMAKE_ARGS+=(
        -DWITH_JEMALLOC=ON
        -DCMAKE_PREFIX_PATH="${JEMALLOC_PREFIX}"
    )
fi

cmake .. "${CMAKE_ARGS[@]}"

# Build static library
echo "Building RocksDB static library..."
make -j"${JOBS}" rocksdb

# Create artifact directory structure
ARTIFACT_DIR="${BUILD_DIR}/artifact"
rm -rf "${ARTIFACT_DIR}"
mkdir -p "${ARTIFACT_DIR}/lib"
mkdir -p "${ARTIFACT_DIR}/include"

# Copy built artifacts
echo "Copying artifacts..."
cp librocksdb.a "${ARTIFACT_DIR}/lib/"
cp -r ../include/rocksdb "${ARTIFACT_DIR}/include/"

# Bundle jemalloc so downstream consumers (ocache and TAG) link the same,
# version-locked allocator from a single artifact.
if [ "${VARIANT}" = "jemalloc" ]; then
    echo "Bundling jemalloc ${JEMALLOC_VERSION}..."
    cp "${JEMALLOC_PREFIX}/lib/libjemalloc.a" "${ARTIFACT_DIR}/lib/"
    cp -r "${JEMALLOC_PREFIX}/include/jemalloc" "${ARTIFACT_DIR}/include/"
fi

# Create metadata file
JEMALLOC_ENABLED=false
JEMALLOC_META_VERSION=""
if [ "${VARIANT}" = "jemalloc" ]; then
    JEMALLOC_ENABLED=true
    JEMALLOC_META_VERSION="${JEMALLOC_VERSION}"
fi
cat > "${ARTIFACT_DIR}/metadata.json" << EOF
{
    "version": "${ROCKSDB_VERSION}",
    "variant": "${VARIANT}",
    "os": "${OS}",
    "arch": "${ARCH}",
    "build_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "static": true,
    "jemalloc": ${JEMALLOC_ENABLED},
    "jemalloc_version": "${JEMALLOC_META_VERSION}"
}
EOF

# Create tarball
cd "${BUILD_DIR}"
TAR_NAME="rocksdb-static-${ROCKSDB_VERSION}${VARIANT_SUFFIX}-${OS}-${ARCH}.tar.gz"
echo "Creating artifact tarball: ${TAR_NAME}"
tar czf "${TAR_NAME}" -C artifact .

echo "Static RocksDB build complete!"
echo "Artifact: ${BUILD_DIR}/${TAR_NAME}"
echo ""
echo "To use this static build:"
echo "1. Extract the tarball to your desired location"
echo "2. Set CGO_CFLAGS=\"-I/path/to/extracted/include\""
if [ "${VARIANT}" = "jemalloc" ]; then
    echo "3. Set CGO_LDFLAGS=\"-L/path/to/extracted/lib -lrocksdb -ljemalloc -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread\""
    echo "   (-ljemalloc makes jemalloc the process allocator; see issue #176)"
else
    echo "3. Set CGO_LDFLAGS=\"-L/path/to/extracted/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread\""
fi
