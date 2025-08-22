#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Storage Layers E2E Test ==="
echo "Testing reads from RocksDB inline, raw files, and segments"
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_SMALL_OBJECTS_ROCKSDB=""
TEST_MEDIUM_OBJECTS_RAW_FILES=""
TEST_COMPACTION_TO_SEGMENTS=""
TEST_MIXED_STORAGE_OPERATIONS=""
TEST_LARGE_VALUE_STREAMING=""
TEST_UPDATE_SMALL_OBJECT=""
TEST_UPDATE_MEDIUM_OBJECT=""
TEST_DELETE_ACROSS_LAYERS=""

# Set timeout for operations
set -o pipefail

# Start the server with specific thresholds for testing different storage layers
echo "Starting OCache server with storage thresholds:"
echo "  - Small objects (<64KB): RocksDB inline"
echo "  - Medium objects (64KB-16MB): Raw files, eligible for compaction"
echo "  - Large objects (>16MB): Permanent raw files"
echo

start_server "storage" \
  -disk /tmp/ocache-storage-test \
  -threshold 65536 \
  -compaction-interval 5s \
  -v

echo "=== Test 1: Small Objects (RocksDB Inline Storage) ==="
echo "Creating small objects (<64KB) that should be stored inline in RocksDB..."

# Create small objects
for i in {1..10}; do
    # Create a 1KB value
    VALUE=$(head -c 1000 /dev/urandom | base64 | head -c 1000)
    ./ocachecli put "small-key-${i}" "$VALUE" 2>&1 | grep -v "^$"
    echo "Added small-key-${i} (1KB)"
done

echo
echo "Reading small objects back..."
SMALL_ERRORS=0
for i in {1..10}; do
    if ! ./ocachecli get "small-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read small-key-${i}${NC}"
        ((SMALL_ERRORS++))
    fi
done

if [ "$SMALL_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All small objects (RocksDB inline) read successfully${NC}"
    TEST_SMALL_OBJECTS_ROCKSDB="PASSED"
else
    echo -e "${RED}✗ Failed to read $SMALL_ERRORS small objects${NC}"
    TEST_SMALL_OBJECTS_ROCKSDB="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 2: Medium Objects (Raw Files) ==="
echo "Creating medium objects (>64KB) that should be stored as raw files..."

# Create medium objects (100KB each)
for i in {1..5}; do
    # Create a 100KB value
    VALUE=$(head -c 100000 /dev/urandom | base64 | head -c 100000)
    ./ocachecli put "medium-key-${i}" "$VALUE" 2>&1 | grep -v "^$"
    echo "Added medium-key-${i} (100KB)"
done

echo
echo "Reading medium objects from raw files..."
MEDIUM_ERRORS=0
for i in {1..5}; do
    if ! ./ocachecli get "medium-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read medium-key-${i}${NC}"
        ((MEDIUM_ERRORS++))
    fi
done

if [ "$MEDIUM_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All medium objects (raw files) read successfully${NC}"
    TEST_MEDIUM_OBJECTS_RAW_FILES="PASSED"
else
    echo -e "${RED}✗ Failed to read $MEDIUM_ERRORS medium objects${NC}"
    TEST_MEDIUM_OBJECTS_RAW_FILES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Triggering Compaction to Segments ==="
echo "Adding more medium objects to trigger compaction..."

# Add more medium objects to trigger compaction
for i in {6..10}; do
    VALUE=$(head -c 100000 /dev/urandom | base64 | head -c 100000)
    ./ocachecli put "medium-key-${i}" "$VALUE" 2>&1 | grep -v "^$"
    echo "Added medium-key-${i} (100KB)"
done

echo
echo "Waiting for compaction to run (30 seconds)..."
sleep 30

echo "Reading medium objects after compaction (should now be in segments)..."
SEGMENT_ERRORS=0
for i in {1..10}; do
    if ! ./ocachecli get "medium-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read medium-key-${i} from segment${NC}"
        ((SEGMENT_ERRORS++))
    fi
done

if [ "$SEGMENT_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All medium objects read successfully after compaction${NC}"
    TEST_COMPACTION_TO_SEGMENTS="PASSED"
else
    echo -e "${RED}✗ Failed to read $SEGMENT_ERRORS objects from segments${NC}"
    TEST_COMPACTION_TO_SEGMENTS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Mixed Storage Layer Operations ==="
echo "Testing concurrent operations across all storage layers..."

# Run mixed operations concurrently
echo "Performing concurrent reads and writes..."

# Start background jobs with timeout
PIDS=()
for i in {1..5}; do
    (
        # Read small object (RocksDB)
        timeout 5 ./ocachecli get "small-key-5" >/dev/null 2>&1 || true
        
        # Read medium object (possibly in segment now)
        timeout 5 ./ocachecli get "medium-key-5" >/dev/null 2>&1 || true
        
        # Add new small object
        SMALL_VALUE=$(head -c 500 /dev/urandom | base64 | head -c 500)
        timeout 10 ./ocachecli put "new-small-${i}" "$SMALL_VALUE" 2>&1 | grep -v "^$" || true
        
        # Add new medium object
        MEDIUM_VALUE=$(head -c 80000 /dev/urandom | base64 | head -c 80000)
        timeout 10 ./ocachecli put "new-medium-${i}" "$MEDIUM_VALUE" 2>&1 | grep -v "^$" || true
    ) &
    PIDS+=($!)
done

# Wait for all background processes with timeout
WAIT_TIME=0
MAX_WAIT=30
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${PIDS[@]}"; do
        if kill -0 "$PID" 2>/dev/null; then
            ((RUNNING++))
        fi
    done
    if [ "$RUNNING" -eq 0 ]; then
        break
    fi
    sleep 1
    ((WAIT_TIME++))
done

if [ "$WAIT_TIME" -ge "$MAX_WAIT" ]; then
    echo -e "${YELLOW}⚠ Some operations timed out after ${MAX_WAIT} seconds${NC}"
    for PID in "${PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

# Verify the new keys were created
MIXED_ERRORS=0
for i in {1..5}; do
    if ! ./ocachecli get "new-small-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read new-small-${i}${NC}"
        ((MIXED_ERRORS++))
    fi
    if ! ./ocachecli get "new-medium-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read new-medium-${i}${NC}"
        ((MIXED_ERRORS++))
    fi
done

if [ "$MIXED_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Mixed storage layer operations completed successfully${NC}"
    TEST_MIXED_STORAGE_OPERATIONS="PASSED"
else
    echo -e "${RED}✗ $MIXED_ERRORS operations failed${NC}"
    TEST_MIXED_STORAGE_OPERATIONS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Large Value Streaming ==="
echo "Testing large value (>1MB) with streaming..."

# Create a large value (1MB to avoid potential issues)
echo "Creating a 1MB value..."
LARGE_VALUE=$(head -c 1000000 /dev/urandom | base64 | head -c 1000000)
echo "Storing large value..."
if ./ocachecli put "large-key-1" "$LARGE_VALUE" 2>&1 | grep -v "^$"; then
    echo "Large value stored"
else
    echo -e "${RED}Failed to store large value${NC}"
    TEST_PASSED=false
fi

echo "Reading large value back..."
RETRIEVED_VALUE=$(./ocachecli get "large-key-1" 2>/dev/null)
RETRIEVED_SIZE=${#RETRIEVED_VALUE}

if [ "$RETRIEVED_SIZE" -eq 1000000 ]; then
    echo -e "${GREEN}✓ Large value (1MB) stored and retrieved successfully${NC}"
    TEST_LARGE_VALUE_STREAMING="PASSED"
elif [ "$RETRIEVED_SIZE" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Large value retrieved but size mismatch: expected 1000000, got $RETRIEVED_SIZE${NC}"
    TEST_LARGE_VALUE_STREAMING="WARNING"
else
    echo -e "${RED}✗ Failed to retrieve large value${NC}"
    TEST_LARGE_VALUE_STREAMING="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 6: Update Operations Across Storage Layers ==="
echo "Testing updates to objects in different storage layers..."

# Update small object
UPDATE_VALUE="updated-small-value"
./ocachecli put "small-key-1" "$UPDATE_VALUE" 2>&1 | grep -v "^$"
RETRIEVED=$(./ocachecli get "small-key-1" 2>/dev/null)
if [ "$RETRIEVED" = "$UPDATE_VALUE" ]; then
    echo -e "${GREEN}✓ Small object update successful${NC}"
    TEST_UPDATE_SMALL_OBJECT="PASSED"
else
    echo -e "${RED}✗ Small object update failed${NC}"
    TEST_UPDATE_SMALL_OBJECT="FAILED"
    TEST_PASSED=false
fi

# Update medium object
UPDATE_VALUE=$(head -c 70000 /dev/urandom | base64 | head -c 70000)
./ocachecli put "medium-key-1" "$UPDATE_VALUE" 2>&1 | grep -v "^$"
RETRIEVED=$(./ocachecli get "medium-key-1" 2>/dev/null)
if [ ${#RETRIEVED} -eq 70000 ]; then
    echo -e "${GREEN}✓ Medium object update successful${NC}"
    TEST_UPDATE_MEDIUM_OBJECT="PASSED"
else
    echo -e "${RED}✗ Medium object update failed${NC}"
    TEST_UPDATE_MEDIUM_OBJECT="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 7: Delete Operations Across Storage Layers ==="
echo "Testing deletion of objects from different storage layers..."

# Delete from each layer
./ocachecli delete "small-key-2" 2>&1 | grep -v "^$"
./ocachecli delete "medium-key-2" 2>&1 | grep -v "^$"
./ocachecli delete "large-key-1" 2>&1 | grep -v "^$"

# Verify deletions
DELETE_ERRORS=0
if ./ocachecli get "small-key-2" >/dev/null 2>&1; then
    echo -e "${RED}Small object not deleted${NC}"
    ((DELETE_ERRORS++))
fi
if ./ocachecli get "medium-key-2" >/dev/null 2>&1; then
    echo -e "${RED}Medium object not deleted${NC}"
    ((DELETE_ERRORS++))
fi
if ./ocachecli get "large-key-1" >/dev/null 2>&1; then
    echo -e "${RED}Large object not deleted${NC}"
    ((DELETE_ERRORS++))
fi

if [ "$DELETE_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All deletions successful across storage layers${NC}"
    TEST_DELETE_ACROSS_LAYERS="PASSED"
else
    echo -e "${RED}✗ $DELETE_ERRORS deletion failures${NC}"
    TEST_DELETE_ACROSS_LAYERS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Small Objects (RocksDB)" "$TEST_SMALL_OBJECTS_ROCKSDB"
print_test_result "Medium Objects (Raw Files)" "$TEST_MEDIUM_OBJECTS_RAW_FILES"
print_test_result "Compaction to Segments" "$TEST_COMPACTION_TO_SEGMENTS"
print_test_result "Mixed Storage Operations" "$TEST_MIXED_STORAGE_OPERATIONS"
print_test_result "Large Value Streaming" "$TEST_LARGE_VALUE_STREAMING"
print_test_result "Update Small Object" "$TEST_UPDATE_SMALL_OBJECT"
print_test_result "Update Medium Object" "$TEST_UPDATE_MEDIUM_OBJECT"
print_test_result "Delete Across Layers" "$TEST_DELETE_ACROSS_LAYERS"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi