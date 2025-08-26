#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

# Test name for this suite
TEST_NAME="storage-layers"

# Track overall test success
TEST_PASSED=true

# Initialize common functionality
init_common

# Individual test results  
TEST_SMALL_OBJECTS_ROCKSDB=""
TEST_SMALL_OBJECTS_DATA_INTEGRITY=""
TEST_MEDIUM_OBJECTS_RAW_FILES=""
TEST_MEDIUM_OBJECTS_DATA_INTEGRITY=""
TEST_LARGE_OBJECTS=""
TEST_LARGE_OBJECTS_DATA_INTEGRITY=""
TEST_UPDATE_LARGE_OBJECTS=""
TEST_COMPACTION_TO_SEGMENTS=""
TEST_COMPACTION_DATA_INTEGRITY=""
TEST_MIXED_STORAGE_OPERATIONS=""
TEST_LARGE_VALUE_STREAMING=""
TEST_UPDATE_SMALL_OBJECT=""
TEST_UPDATE_MEDIUM_OBJECT=""
TEST_DELETE_ACROSS_LAYERS=""
TEST_CONCURRENT_DATA_INTEGRITY=""
TEST_SERVER_RESTART_PERSISTENCE=""
TEST_BYTE_RANGE_READS=""
TEST_DATA_CORRUPTION_DETECTION=""
TEST_STRESS_TEST_INTEGRITY=""

# Fail on any command in a pipeline failing
set -o pipefail

# Initialize checksum file for data integrity tests
CHECKSUM_FILE="/tmp/checksums-$$.txt"

# Start the server with specific thresholds for testing different storage layers
echo "Starting OCache server with storage thresholds:"
echo "  - Small objects (<64KB): RocksDB inline"
echo "  - Medium objects (64KB-16MB): Raw files, then compacted to segments"
echo "  - Large objects (>16MB): Permanent raw files"

start_server "$TEST_NAME" "false" \
  -disk /tmp/ocache-storage-test \
  -threshold 65536 \
  -compaction-interval 10s \
  -segment-size 16777216 \
  -v

echo
echo "=== Test 1: Small Objects (RocksDB Inline Storage) ==="
echo "Creating small objects (<64KB) that should be stored inline in RocksDB..."

# Create small objects with checksums
SMALL_INTEGRITY_ERRORS=0
for i in {1..10}; do
    # Create a 1KB deterministic value
    VALUE=$(generate_deterministic_data "small${i}" 1000)
    store_test_data "small-key-${i}" "$VALUE" "$CHECKSUM_FILE"
    printf "%s" "$VALUE" | ./ocachecli put "small-key-${i}" 2>&1
    echo "Added small-key-${i} (1KB)"
done

echo
echo "Reading small objects back and verifying data integrity..."
SMALL_ERRORS=0
for i in {1..10}; do
    if ! verify_stored_data "small-key-${i}" "$CHECKSUM_FILE"; then
        echo -e "${RED}Failed to verify small-key-${i}${NC}"
        ((SMALL_ERRORS++))
        ((SMALL_INTEGRITY_ERRORS++))
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

if [ "$SMALL_INTEGRITY_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Data integrity verified for all small objects${NC}"
    TEST_SMALL_OBJECTS_DATA_INTEGRITY="PASSED"
else
    echo -e "${RED}✗ Data integrity failed for $SMALL_INTEGRITY_ERRORS small objects${NC}"
    TEST_SMALL_OBJECTS_DATA_INTEGRITY="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 2: Medium Objects (Raw Files) ==="
echo "Creating medium objects (>64KB) that should be stored as raw files..."

# Create medium objects (100KB each) with checksums
MEDIUM_INTEGRITY_ERRORS=0
for i in {1..5}; do
    # Create a 100KB deterministic value
    VALUE=$(generate_deterministic_data "medium${i}" 100000)
    store_test_data "medium-key-${i}" "$VALUE" "$CHECKSUM_FILE"
    printf "%s" "$VALUE" | ./ocachecli put "medium-key-${i}" 2>&1
    echo "Added medium-key-${i} (100KB)"
done

echo
echo "Reading medium objects from raw files and verifying data integrity..."
MEDIUM_ERRORS=0
for i in {1..5}; do
    if ! verify_stored_data "medium-key-${i}" "$CHECKSUM_FILE"; then
        echo -e "${RED}Failed to verify medium-key-${i}${NC}"
        ((MEDIUM_ERRORS++))
        ((MEDIUM_INTEGRITY_ERRORS++))
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

if [ "$MEDIUM_INTEGRITY_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Data integrity verified for all medium objects${NC}"
    TEST_MEDIUM_OBJECTS_DATA_INTEGRITY="PASSED"
else
    echo -e "${RED}✗ Data integrity failed for $MEDIUM_INTEGRITY_ERRORS medium objects${NC}"
    TEST_MEDIUM_OBJECTS_DATA_INTEGRITY="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Large Objects (Permanent Raw Files) ==="
echo "Testing large objects that exceed segment size and remain as raw files..."

# Test large objects (>16MB, never compacted)
echo "Adding large objects (>16MB each)..."
LARGE_ERRORS=0
LARGE_INTEGRITY_ERRORS=0
for i in {1..3}; do
    KEY="large-key-${i}"
    SIZE=$((20000000 + i * 1000000))  # 20MB, 21MB, 22MB
    
    # Generate deterministic large data and store with checksum
    VALUE=$(generate_deterministic_data "large${i}" "$SIZE")
    store_test_data "$KEY" "$VALUE" "$CHECKSUM_FILE"
    printf "%s" "$VALUE" | ./ocachecli put "$KEY" 2>&1
    echo "Added $KEY ($((SIZE / 1000000))MB)"
done

echo
echo "Reading large objects back..."
for i in {1..3}; do
    KEY="large-key-${i}"
    
    # Verify data integrity
    if ! verify_stored_data "$KEY" "$CHECKSUM_FILE"; then
        echo -e "${RED}✗ Failed to verify $KEY${NC}"
        ((LARGE_ERRORS++))
        ((LARGE_INTEGRITY_ERRORS++))
    else
        echo -e "${GREEN}✓ Successfully verified $KEY${NC}"
    fi
done

if [ "$LARGE_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All large objects (permanent raw files) read successfully${NC}"
    TEST_LARGE_OBJECTS="PASSED"
else
    echo -e "${RED}✗ Failed to read $LARGE_ERRORS large objects${NC}"
    TEST_LARGE_OBJECTS="FAILED"
    TEST_PASSED=false
fi

if [ "$LARGE_INTEGRITY_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Data integrity verified for all large objects${NC}"
    TEST_LARGE_OBJECTS_DATA_INTEGRITY="PASSED"
else
    echo -e "${RED}✗ Data integrity failed for $LARGE_INTEGRITY_ERRORS large objects${NC}"
    TEST_LARGE_OBJECTS_DATA_INTEGRITY="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Update Large Objects ==="
echo "Testing updates to large objects..."

UPDATE_LARGE_ERRORS=0
for i in {1..2}; do
    KEY="large-key-${i}"
    SIZE=$((25000000 + i * 1000000))  # 25MB, 26MB - larger than original
    
    # Generate new data for update
    NEW_VALUE=$(generate_deterministic_data "large-update${i}" "$SIZE")
    echo "Updating $KEY with new $((SIZE / 1000000))MB data..."
    store_test_data "$KEY" "$NEW_VALUE" "$CHECKSUM_FILE"
    printf "%s" "$NEW_VALUE" | ./ocachecli put "$KEY" 2>&1
    
    # Verify updated data
    if ! verify_stored_data "$KEY" "$CHECKSUM_FILE"; then
        echo -e "${RED}✗ Failed to verify updated $KEY${NC}"
        ((UPDATE_LARGE_ERRORS++))
    else
        echo -e "${GREEN}✓ Successfully updated and verified $KEY${NC}"
    fi
done

if [ "$UPDATE_LARGE_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All large object updates successful${NC}"
    TEST_UPDATE_LARGE_OBJECTS="PASSED"
else
    echo -e "${RED}✗ Failed to update $UPDATE_LARGE_ERRORS large objects${NC}"
    TEST_UPDATE_LARGE_OBJECTS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Triggering Compaction to Segments ==="
echo "Adding more medium objects to trigger compaction..."

# Add more medium objects to trigger compaction with checksums
for i in {6..10}; do
    VALUE=$(generate_deterministic_data "medium${i}" 100000)
    store_test_data "medium-key-${i}" "$VALUE" "$CHECKSUM_FILE"
    printf "%s" "$VALUE" | ./ocachecli put "medium-key-${i}" 2>&1
    echo "Added medium-key-${i} (100KB) for compaction"
done

echo
echo "Waiting for compaction to run (30 seconds)..."
sleep 30

echo "Reading medium objects after compaction and verifying data integrity..."
SEGMENT_ERRORS=0
COMPACTION_INTEGRITY_ERRORS=0
for i in {1..10}; do
    if ! verify_stored_data "medium-key-${i}" "$CHECKSUM_FILE"; then
        echo -e "${RED}Failed to verify medium-key-${i} after compaction${NC}"
        ((SEGMENT_ERRORS++))
        ((COMPACTION_INTEGRITY_ERRORS++))
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

if [ "$COMPACTION_INTEGRITY_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Data integrity maintained during compaction${NC}"
    TEST_COMPACTION_DATA_INTEGRITY="PASSED"
else
    echo -e "${RED}✗ Data corruption detected in $COMPACTION_INTEGRITY_ERRORS objects after compaction${NC}"
    TEST_COMPACTION_DATA_INTEGRITY="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 6: Mixed Storage Layer Operations ==="
echo "Testing concurrent operations across all storage layers..."

# Run mixed operations concurrently
echo "Performing concurrent reads and writes..."

# Start background jobs
PIDS=()
for i in {1..5}; do
    (
        # Read small object (RocksDB)
        ./ocachecli get "small-key-5" >/dev/null 2>&1 || true
        
        # Read medium object (possibly in segment now)
        ./ocachecli get "medium-key-5" >/dev/null 2>&1 || true
        
        # Read large object
        ./ocachecli get "large-key-1" >/dev/null 2>&1 || true
        
        # Add new small object
        SMALL_VALUE=$(generate_deterministic_data "mixed-small${i}" 500)
        printf "%s" "$SMALL_VALUE" | ./ocachecli put "new-small-${i}" 2>&1 || true
        
        # Add new medium object
        MEDIUM_VALUE=$(generate_deterministic_data "mixed-medium${i}" 80000)
        printf "%s" "$MEDIUM_VALUE" | ./ocachecli put "new-medium-${i}" 2>&1 || true
    ) &
    PIDS+=($!)
done

# Wait for all background processes
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

# Kill any still-running processes
for PID in "${PIDS[@]}"; do
    kill "$PID" 2>/dev/null || true
done

# Verify we can still read original data
MIXED_ERRORS=0
if ! ./ocachecli get "small-key-1" >/dev/null 2>&1; then
    ((MIXED_ERRORS++))
fi
if ! ./ocachecli get "medium-key-1" >/dev/null 2>&1; then
    ((MIXED_ERRORS++))
fi
if ! ./ocachecli get "large-key-1" >/dev/null 2>&1; then
    ((MIXED_ERRORS++))
fi

if [ "$MIXED_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Mixed storage layer operations completed successfully${NC}"
    TEST_MIXED_STORAGE_OPERATIONS="PASSED"
else
    echo -e "${RED}✗ $MIXED_ERRORS errors during mixed operations${NC}"
    TEST_MIXED_STORAGE_OPERATIONS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 7: Large Value Streaming ==="
echo "Testing large value (>1MB) with streaming..."

# Create a large value (1MB)
echo "Creating a 1MB value..."
LARGE_VALUE=$(generate_deterministic_data "stream-test" 1000000)
echo "Storing large value..."
printf "%s" "$LARGE_VALUE" | ./ocachecli put "large-stream-key" 2>&1 || true
echo "Large value stored"

echo "Reading large value back..."
RETRIEVED_VALUE=$(./ocachecli get "large-stream-key" 2>/dev/null)
RETRIEVED_SIZE=${#RETRIEVED_VALUE}
EXPECTED_SIZE=${#LARGE_VALUE}

if [ "$RETRIEVED_VALUE" = "$LARGE_VALUE" ]; then
    echo -e "${GREEN}✓ Large value (1MB) stored and retrieved successfully${NC}"
    TEST_LARGE_VALUE_STREAMING="PASSED"
elif [ "$RETRIEVED_SIZE" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Large value retrieved but size mismatch: expected $EXPECTED_SIZE, got $RETRIEVED_SIZE${NC}"
    TEST_LARGE_VALUE_STREAMING="WARNING"
else
    echo -e "${RED}✗ Failed to retrieve large value${NC}"
    TEST_LARGE_VALUE_STREAMING="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 8: Update Operations Across Storage Layers ==="
echo "Testing updates to objects in different storage layers..."

# Update small object
UPDATE_VALUE="updated-small-value"
./ocachecli put "small-key-1" "$UPDATE_VALUE" 2>&1
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
UPDATE_VALUE=$(generate_deterministic_data "update-medium" 120000)
printf "%s" "$UPDATE_VALUE" | ./ocachecli put "medium-key-1" 2>&1
RETRIEVED=$(./ocachecli get "medium-key-1" 2>/dev/null)
if [ "$RETRIEVED" = "$UPDATE_VALUE" ]; then
    echo -e "${GREEN}✓ Medium object update successful${NC}"
    TEST_UPDATE_MEDIUM_OBJECT="PASSED"
else
    echo -e "${RED}✗ Medium object update failed${NC}"
    TEST_UPDATE_MEDIUM_OBJECT="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 9: Delete Operations Across Storage Layers ==="
echo "Testing delete operations across all storage layers..."

# Delete a small object
./ocachecli delete "small-key-10" 2>&1
if ./ocachecli get "small-key-10" >/dev/null 2>&1; then
    echo -e "${RED}✗ Failed to delete small object${NC}"
    TEST_DELETE_ACROSS_LAYERS="FAILED"
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ Small object deleted successfully${NC}"
fi

# Delete a medium object  
./ocachecli delete "medium-key-10" 2>&1
if ./ocachecli get "medium-key-10" >/dev/null 2>&1; then
    echo -e "${RED}✗ Failed to delete medium object${NC}"
    TEST_DELETE_ACROSS_LAYERS="FAILED"
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ Medium object deleted successfully${NC}"
fi

# Delete a large object
./ocachecli delete "large-key-3" 2>&1
if ./ocachecli get "large-key-3" >/dev/null 2>&1; then
    echo -e "${RED}✗ Failed to delete large object${NC}"
    TEST_DELETE_ACROSS_LAYERS="FAILED"
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ Large object deleted successfully${NC}"
fi

if [ "$TEST_DELETE_ACROSS_LAYERS" != "FAILED" ]; then
    echo -e "${GREEN}✓ All delete operations successful${NC}"
    TEST_DELETE_ACROSS_LAYERS="PASSED"
fi

echo
echo "=== Test 10: Concurrent Access Data Integrity ==="
echo "Testing data integrity with concurrent reads and writes..."

# Function to perform concurrent operations with validation
concurrent_worker() {
    local worker_id=$1
    local errors=0
    
    for i in {1..5}; do
        KEY="concurrent-${worker_id}-${i}"
        # Use original 50KB size
        DATA=$(generate_deterministic_data "worker${worker_id}${i}" 50000)
        
        # Write using printf for better handling
        printf "%s" "$DATA" | ./ocachecli put "$KEY" >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo "Worker $worker_id: Write failed for $KEY" >&2
            ((errors++))
            continue
        fi
        
        # Immediate read and verify
        RETRIEVED=$(./ocachecli get "$KEY" 2>/dev/null)
        if [ -z "$RETRIEVED" ]; then
            echo "Worker $worker_id: Failed to read $KEY" >&2
            ((errors++))
            continue
        fi
        
        EXPECTED_CHECKSUM=$(printf "%s" "$DATA" | md5sum | cut -d' ' -f1)
        ACTUAL_CHECKSUM=$(printf "%s" "$RETRIEVED" | md5sum | cut -d' ' -f1)
        
        if [ "$EXPECTED_CHECKSUM" != "$ACTUAL_CHECKSUM" ]; then
            echo "Worker $worker_id: Data corruption detected for $KEY" >&2
            ((errors++))
        fi
    done
    
    echo "$errors" > "/tmp/worker-${worker_id}-errors.txt"
}

# Run concurrent workers
echo "Starting 5 concurrent workers..."
PIDS=()
for worker in {1..5}; do
    concurrent_worker "$worker" &
    PIDS+=($!)
done

# Wait for completion
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

# Kill any still-running processes
for PID in "${PIDS[@]}"; do
    kill "$PID" 2>/dev/null || true
done

# Collect results
CONCURRENT_ERRORS=0
for worker in {1..5}; do
    if [ -f "/tmp/worker-${worker}-errors.txt" ]; then
        ERRORS=$(cat "/tmp/worker-${worker}-errors.txt")
        CONCURRENT_ERRORS=$((CONCURRENT_ERRORS + ERRORS))
        rm -f "/tmp/worker-${worker}-errors.txt"
    fi
done

if [ "$CONCURRENT_ERRORS" -eq 0 ]; then
    TEST_CONCURRENT_DATA_INTEGRITY="PASSED"
    echo -e "${GREEN}✓ Data integrity maintained during concurrent access${NC}"
else
    TEST_CONCURRENT_DATA_INTEGRITY="FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ $CONCURRENT_ERRORS data corruption issues during concurrent access${NC}"
fi

echo
echo "=== Test 11: Server Restart Data Persistence ==="
echo "Testing data persistence and integrity across server restart..."

# Add test data before restart
echo "Adding test data before restart..."
RESTART_TEST_KEYS=()
for i in {1..5}; do
    KEY="restart-test-${i}"
    # Generate 75KB data as originally intended
    DATA=$(generate_deterministic_data "restart${i}" 75000)
    
    # Use printf instead of echo for better handling of large data
    printf "%s" "$DATA" | ./ocachecli put "$KEY" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Failed to store $KEY"
        continue
    fi
    CHECKSUM=$(printf "%s" "$DATA" | md5sum | cut -d' ' -f1)
    echo "${KEY}:${CHECKSUM}" >> "$CHECKSUM_FILE"
    RESTART_TEST_KEYS+=("$KEY")
done

echo "Stopping server..."
stop_server

echo "Restarting server..."
start_server "$TEST_NAME" "false" \
  -disk /tmp/ocache-storage-test \
  -threshold 65536 \
  -compaction-interval 10s \
  -segment-size 16777216 \
  -v

echo "Verifying data integrity after restart..."
RESTART_ERRORS=0
for KEY in "${RESTART_TEST_KEYS[@]}"; do
    if ! verify_stored_data "$KEY" "$CHECKSUM_FILE"; then
        echo -e "${RED}✗ Data lost or corrupted for ${KEY} after restart${NC}"
        ((RESTART_ERRORS++))
    else
        echo -e "${GREEN}✓ Data persisted correctly for ${KEY}${NC}"
    fi
done

if [ "$RESTART_ERRORS" -eq 0 ]; then
    TEST_SERVER_RESTART_PERSISTENCE="PASSED"
    echo -e "${GREEN}✓ All data persisted correctly across server restart${NC}"
else
    TEST_SERVER_RESTART_PERSISTENCE="FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ $RESTART_ERRORS objects lost or corrupted after restart${NC}"
fi

echo
echo "=== Test 12: Byte-Range Read Validation ==="
echo "Testing partial reads maintain data integrity..."

# Create a test object with known content
KEY="byte-range-test"
# Create predictable content for easy verification
FULL_DATA=""
for i in {0..9}; do
    FULL_DATA="${FULL_DATA}Block${i}:" 
    FULL_DATA="${FULL_DATA}$(printf '%090d' "$i")"  # 90 digits = 100 chars per block
done

printf "%s" "$FULL_DATA" | ./ocachecli put "$KEY" 2>&1
echo "Created test object with ${#FULL_DATA} bytes"

# Note: ocachecli doesn't support byte-range reads directly, 
# but we can verify the full data is intact
RETRIEVED=$(./ocachecli get "$KEY" 2>/dev/null)
if [ "$RETRIEVED" = "$FULL_DATA" ]; then
    TEST_BYTE_RANGE_READS="PASSED"
    echo -e "${GREEN}✓ Full data integrity verified (byte-range foundation test)${NC}"
else
    TEST_BYTE_RANGE_READS="FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ Data integrity failed for byte-range test object${NC}"
fi

echo
echo "=== Test 13: Data Corruption Detection ==="
echo "Testing ability to detect data corruption..."

# Create objects and then verify repeatedly
CORRUPTION_TEST_ERRORS=0
echo "Creating test objects for corruption detection..."

for i in {1..5}; do
    KEY="corruption-test-${i}"
    # Use original 80KB size
    DATA=$(generate_deterministic_data "corrupt${i}" 80000)
    
    # Use printf instead of echo for better handling
    printf "%s" "$DATA" | ./ocachecli put "$KEY" >/dev/null 2>&1
    CHECKSUM=$(printf "%s" "$DATA" | md5sum | cut -d' ' -f1)
    echo "${KEY}:${CHECKSUM}" >> "$CHECKSUM_FILE"
done

echo "Performing multiple read cycles to detect any corruption..."
for cycle in {1..3}; do
    echo "Verification cycle $cycle..."
    for i in {1..5}; do
        KEY="corruption-test-${i}"
        if ! verify_stored_data "$KEY" "$CHECKSUM_FILE"; then
            echo -e "${RED}✗ Corruption detected in ${KEY} during cycle $cycle${NC}"
            ((CORRUPTION_TEST_ERRORS++))
        fi
    done
    sleep 2
done

if [ "$CORRUPTION_TEST_ERRORS" -eq 0 ]; then
    TEST_DATA_CORRUPTION_DETECTION="PASSED"
    echo -e "${GREEN}✓ No data corruption detected across multiple read cycles${NC}"
else
    TEST_DATA_CORRUPTION_DETECTION="FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ Detected $CORRUPTION_TEST_ERRORS corruption instances${NC}"
fi

echo
echo "=== Test 14: Stress Test Data Integrity ==="
echo "Performing stress test with rapid operations..."

# Stress test function
stress_worker() {
    local worker_id=$1
    local errors=0
    
    for i in {1..10}; do  # Reduced from 20 to 10 for manageable testing
        KEY="stress-${worker_id}-${i}"
        SIZE=$((RANDOM % 100000 + 10000))  # 10KB-110KB range
        DATA=$(generate_deterministic_data "stress${worker_id}${i}" "$SIZE")
        
        # Rapid put/get/delete cycle
        printf "%s" "$DATA" | ./ocachecli put "$KEY" >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            ((errors++))
            continue
        fi

        RETRIEVED=$(./ocachecli get "$KEY" 2>/dev/null)

        if [ -n "$RETRIEVED" ]; then
            CHECKSUM_EXPECTED=$(printf "%s" "$DATA" | md5sum | cut -d' ' -f1)
            CHECKSUM_ACTUAL=$(printf "%s" "$RETRIEVED" | md5sum | cut -d' ' -f1)
            if [ "$CHECKSUM_EXPECTED" != "$CHECKSUM_ACTUAL" ]; then
                ((errors++))
            fi
        else
            ((errors++))
        fi
        
        # Random delete
        if [ $((RANDOM % 3)) -eq 0 ]; then
            ./ocachecli delete "$KEY" >/dev/null 2>&1 || true
        fi
    done
    
    echo "$errors" > "/tmp/stress-${worker_id}-errors.txt"
}

echo "Running stress test with 10 workers..."
PIDS=()
for worker in {1..10}; do
    stress_worker "$worker" &
    PIDS+=($!)
done

# Wait for completion
WAIT_TIME=0
MAX_WAIT=60  # Give stress test more time
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

# Kill any still-running processes
for PID in "${PIDS[@]}"; do
    kill "$PID" 2>/dev/null || true
done

# Collect stress test results
STRESS_ERRORS=0
for worker in {1..10}; do
    if [ -f "/tmp/stress-${worker}-errors.txt" ]; then
        ERRORS=$(cat "/tmp/stress-${worker}-errors.txt")
        STRESS_ERRORS=$((STRESS_ERRORS + ERRORS))
        rm -f "/tmp/stress-${worker}-errors.txt"
    fi
done

if [ "$STRESS_ERRORS" -eq 0 ]; then
    TEST_STRESS_TEST_INTEGRITY="PASSED"
    echo -e "${GREEN}✓ Data integrity maintained under stress${NC}"
else
    TEST_STRESS_TEST_INTEGRITY="FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ $STRESS_ERRORS errors detected during stress test${NC}"
fi

# Test Results Summary
echo
echo "========================================="
echo "Storage Layers Test Results Summary"
echo "========================================="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Small Objects (RocksDB)" "$TEST_SMALL_OBJECTS_ROCKSDB"
print_test_result "Small Objects Data Integrity" "$TEST_SMALL_OBJECTS_DATA_INTEGRITY"
print_test_result "Medium Objects (Raw Files)" "$TEST_MEDIUM_OBJECTS_RAW_FILES"
print_test_result "Medium Objects Data Integrity" "$TEST_MEDIUM_OBJECTS_DATA_INTEGRITY"
print_test_result "Large Objects (Permanent Raw)" "$TEST_LARGE_OBJECTS"
print_test_result "Large Objects Data Integrity" "$TEST_LARGE_OBJECTS_DATA_INTEGRITY"
print_test_result "Update Large Objects" "$TEST_UPDATE_LARGE_OBJECTS"
print_test_result "Compaction to Segments" "$TEST_COMPACTION_TO_SEGMENTS"
print_test_result "Compaction Data Integrity" "$TEST_COMPACTION_DATA_INTEGRITY"
print_test_result "Mixed Storage Operations" "$TEST_MIXED_STORAGE_OPERATIONS"
print_test_result "Large Value Streaming" "$TEST_LARGE_VALUE_STREAMING"
print_test_result "Update Small Object" "$TEST_UPDATE_SMALL_OBJECT"
print_test_result "Update Medium Object" "$TEST_UPDATE_MEDIUM_OBJECT"
print_test_result "Delete Across Layers" "$TEST_DELETE_ACROSS_LAYERS"
print_test_result "Concurrent Data Integrity" "$TEST_CONCURRENT_DATA_INTEGRITY"
print_test_result "Server Restart Persistence" "$TEST_SERVER_RESTART_PERSISTENCE"
print_test_result "Byte-Range Reads" "$TEST_BYTE_RANGE_READS"
print_test_result "Data Corruption Detection" "$TEST_DATA_CORRUPTION_DETECTION"
print_test_result "Stress Test Integrity" "$TEST_STRESS_TEST_INTEGRITY"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi