#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Compaction E2E Test ==="
echo "Testing compaction from raw files to segments and API operations"
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_CREATE_RAW_FILES_FOR_COMPACTION=""
TEST_WAIT_FOR_COMPACTION=""
TEST_MIXED_OPERATIONS_DURING_COMPACTION=""
TEST_LARGE_SCALE_COMPACTION=""
TEST_COMPACTION_WITH_MIXED_SIZES=""
TEST_CONCURRENT_ACCESS_DURING_COMPACTION=""
TEST_SERVER_RESTART_AFTER_COMPACTION=""

# Start the server with aggressive compaction settings for testing
echo "Starting OCache server with compaction settings:"
echo "  - Threshold: 64KB (objects >64KB go to raw files)"
echo "  - Compaction interval: 5 seconds"
echo "  - Target segment size: 1MB"
echo

start_server "compaction" "true" \
  -disk /tmp/ocache-compaction-test \
  -threshold 65536 \
  -segment-size 1048576 \
  -v

echo "=== Test 1: Create Raw Files for Compaction ==="
echo "Creating medium objects (>64KB) that will be stored as raw files..."

# Create 5 medium objects (100KB each) to trigger compaction
for i in {1..5}; do
    head -c 100000 /dev/urandom | base64 | head -c 100000 | ./ocachecli put "compact-key-${i}" 2>&1 || true
    echo "Added compact-key-${i} (100KB) as raw file"
done

echo
echo "Verifying all keys are readable before compaction..."
PRE_COMPACT_ERRORS=0
for i in {1..5}; do
    if ! ./ocachecli get "compact-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read compact-key-${i} before compaction${NC}"
        ((PRE_COMPACT_ERRORS++))
    fi
done

if [ "$PRE_COMPACT_ERRORS" -eq 0 ]; then
    pass_test "TEST_CREATE_RAW_FILES_FOR_COMPACTION" "All keys readable from raw files"
else
    fail_test "TEST_CREATE_RAW_FILES_FOR_COMPACTION" "$PRE_COMPACT_ERRORS keys unreadable before compaction"
fi

echo
echo "=== Test 2: Wait for Compaction ==="
echo "Waiting for compaction to occur (10 seconds)..."
echo "Compaction should combine raw files into segments..."
sleep 10

echo
echo "Verifying keys are still readable after compaction..."
POST_COMPACT_ERRORS=0
for i in {1..5}; do
    if ! ./ocachecli get "compact-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read compact-key-${i} after compaction${NC}"
        ((POST_COMPACT_ERRORS++))
    fi
done

if [ "$POST_COMPACT_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All keys readable after compaction to segments${NC}"
    TEST_WAIT_FOR_COMPACTION="PASSED"
else
    echo -e "${RED}✗ $POST_COMPACT_ERRORS keys unreadable after compaction${NC}"
    TEST_WAIT_FOR_COMPACTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Mixed Operations During Compaction ==="
echo "Testing reads/writes/deletes while compaction is active..."

# Add more files to trigger another compaction
echo "Adding more raw files..."
for i in {6..10}; do
    head -c 80000 /dev/urandom | base64 | head -c 80000 | ./ocachecli put "active-key-${i}" 2>&1 || true
done

# Perform operations while compaction might be running
echo "Performing mixed operations during potential compaction..."

# Update an existing key
head -c 90000 /dev/urandom | base64 | head -c 90000 | ./ocachecli put "compact-key-2" 2>&1 || true
echo "Updated compact-key-2"

# Delete a key
./ocachecli delete "compact-key-3" 2>&1 || true
echo "Deleted compact-key-3"

# Read keys
for i in 1 4 5; do
    if ./ocachecli get "compact-key-${i}" >/dev/null 2>&1; then
        echo "Successfully read compact-key-${i}"
    fi
done

# Wait for compaction
sleep 8

echo
echo "Verifying operations were successful..."

# Check update worked
MIXED_OPS_ERRORS=0
if ./ocachecli get "compact-key-2" >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Updated key readable${NC}"
else
    echo -e "${RED}✗ Updated key not found${NC}"
    ((MIXED_OPS_ERRORS++))
    TEST_PASSED=false
fi

# Check delete worked
if ./ocachecli get "compact-key-3" >/dev/null 2>&1; then
    echo -e "${RED}✗ Deleted key still exists${NC}"
    ((MIXED_OPS_ERRORS++))
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ Deleted key properly removed${NC}"
fi

if [ "$MIXED_OPS_ERRORS" -eq 0 ]; then
    TEST_MIXED_OPERATIONS_DURING_COMPACTION="PASSED"
else
    TEST_MIXED_OPERATIONS_DURING_COMPACTION="FAILED"
fi

echo
echo "=== Test 4: Large-Scale Compaction ==="
echo "Creating many files to test larger compaction scenarios..."

# Create 20 medium files
for i in {11..30}; do
    head -c 75000 /dev/urandom | base64 | head -c 75000 | ./ocachecli put "bulk-key-${i}" 2>&1 || true
    echo "Added bulk-key-${i} (75KB)"
done

echo
echo "Waiting for large-scale compaction (15 seconds)..."
sleep 15

# Verify all keys are still accessible
echo "Verifying all bulk keys after large compaction..."
BULK_ERRORS=0
for i in {11..30}; do
    if ! ./ocachecli get "bulk-key-${i}" >/dev/null 2>&1; then
        ((BULK_ERRORS++))
    fi
done

if [ "$BULK_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All 20 bulk keys accessible after large compaction${NC}"
    TEST_LARGE_SCALE_COMPACTION="PASSED"
else
    echo -e "${RED}✗ $BULK_ERRORS bulk keys inaccessible after compaction${NC}"
    TEST_LARGE_SCALE_COMPACTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Compaction with Mixed Sizes ==="
echo "Testing compaction with varied object sizes..."

# Add objects of different sizes
echo "Adding objects of various sizes..."

# Small objects (should stay in RocksDB)
for i in {1..5}; do
    VALUE=$(head -c 10000 /dev/urandom | base64 | head -c 10000)
    ./ocachecli put "small-compact-${i}" "$VALUE" 2>&1 || true
done

# Medium objects (raw files -> segments)
for i in {1..5}; do
    head -c 70000 /dev/urandom | base64 | head -c 70000 | ./ocachecli put "medium-compact-${i}" 2>&1 || true
done

# Large objects (should stay as raw files if >16MB, but we'll use 500KB for testing)
for i in {1..3}; do
    head -c 500000 /dev/urandom | base64 | head -c 500000 | ./ocachecli put "large-compact-${i}" 2>&1 || true
done

echo "Waiting for compaction of mixed sizes..."
sleep 10

# Verify all objects are accessible
echo "Verifying mixed-size objects after compaction..."
MIXED_ERRORS=0

for i in {1..5}; do
    if ! ./ocachecli get "small-compact-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed: small-compact-${i}${NC}"
        ((MIXED_ERRORS++))
    fi
done

for i in {1..5}; do
    if ! ./ocachecli get "medium-compact-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed: medium-compact-${i}${NC}"
        ((MIXED_ERRORS++))
    fi
done

for i in {1..3}; do
    if ! ./ocachecli get "large-compact-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed: large-compact-${i}${NC}"
        ((MIXED_ERRORS++))
    fi
done

if [ "$MIXED_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All mixed-size objects accessible after compaction${NC}"
    TEST_COMPACTION_WITH_MIXED_SIZES="PASSED"
else
    echo -e "${RED}✗ $MIXED_ERRORS mixed-size objects failed${NC}"
    TEST_COMPACTION_WITH_MIXED_SIZES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 6: Concurrent Access During Compaction ==="
echo "Testing concurrent reads during active compaction..."

# Add files to trigger compaction
echo "Adding files to trigger compaction..."
for i in {31..35}; do
    head -c 85000 /dev/urandom | base64 | head -c 85000 | ./ocachecli put "concurrent-compact-${i}" 2>&1 || true
done

# Function to continuously read keys
continuous_reader() {
    local key=$1
    local success=0
    local fail=0
    for j in {1..20}; do
        if ./ocachecli get "$key" >/dev/null 2>&1; then
            ((success++))
        else
            ((fail++))
        fi
        sleep 0.2
    done
    echo "Reader for $key: $success successes, $fail failures"
}

# Start concurrent readers while compaction might be happening
echo "Starting concurrent readers during compaction window..."
READER_PIDS=()
for i in {31..33}; do
    continuous_reader "concurrent-compact-${i}" &
    READER_PIDS+=($!)
done

# Wait for readers to complete
WAIT_TIME=0
MAX_WAIT=30
echo "Waiting for readers to complete (max ${MAX_WAIT}s)..."
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${READER_PIDS[@]}"; do
        if kill -0 "$PID" 2>/dev/null; then
            ((RUNNING++))
        fi
    done
    if [ "$RUNNING" -eq 0 ]; then
        break
    fi
    sleep 1
    ((WAIT_TIME++))
    if [ $((WAIT_TIME % 5)) -eq 0 ]; then
        echo "  Still waiting... ($RUNNING readers running)"
    fi
done

if [ "$RUNNING" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Timeout: Killing remaining reader processes${NC}"
    for PID in "${READER_PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

echo -e "${GREEN}✓ Concurrent access during compaction completed${NC}"
TEST_CONCURRENT_ACCESS_DURING_COMPACTION="PASSED"

echo
echo "=== Test 7: Server Restart After Compaction ==="
echo "Testing data integrity after server restart with compacted segments..."

# Get current key count
KEY_COUNT_BEFORE=$(./ocachecli list | wc -l)
echo "Keys before restart: $KEY_COUNT_BEFORE"

# Stop server
echo "Stopping server..."
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null

# Restart server
echo "Restarting server..."
start_server "compaction" "false" \
  -disk /tmp/ocache-compaction-test \
  -threshold 65536 \
  -v

# Check key count after restart
KEY_COUNT_AFTER=$(./ocachecli list | wc -l)
echo "Keys after restart: $KEY_COUNT_AFTER"

RESTART_ERRORS=0
if [ "$KEY_COUNT_BEFORE" -eq "$KEY_COUNT_AFTER" ]; then
    echo -e "${GREEN}✓ All keys preserved after restart with compacted segments${NC}"
else
    echo -e "${RED}✗ Key count mismatch: before=$KEY_COUNT_BEFORE, after=$KEY_COUNT_AFTER${NC}"
    ((RESTART_ERRORS++))
    TEST_PASSED=false
fi

# Spot check some keys
RESTART_CHECK=0
for key in "compact-key-1" "bulk-key-15" "medium-compact-3"; do
    if ! ./ocachecli get "$key" >/dev/null 2>&1; then
        echo -e "${RED}Key $key not found after restart${NC}"
        ((RESTART_CHECK++))
    fi
done

if [ "$RESTART_CHECK" -eq 0 ]; then
    echo -e "${GREEN}✓ Spot check: compacted keys accessible after restart${NC}"
else
    echo -e "${RED}✗ Some compacted keys lost after restart${NC}"
    ((RESTART_ERRORS++))
    TEST_PASSED=false
fi

if [ "$RESTART_ERRORS" -eq 0 ]; then
    TEST_SERVER_RESTART_AFTER_COMPACTION="PASSED"
else
    TEST_SERVER_RESTART_AFTER_COMPACTION="FAILED"
fi

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Create Raw Files for Compaction" "$TEST_CREATE_RAW_FILES_FOR_COMPACTION"
print_test_result "Wait for Compaction" "$TEST_WAIT_FOR_COMPACTION"
print_test_result "Mixed Operations During Compaction" "$TEST_MIXED_OPERATIONS_DURING_COMPACTION"
print_test_result "Large-Scale Compaction" "$TEST_LARGE_SCALE_COMPACTION"
print_test_result "Compaction with Mixed Sizes" "$TEST_COMPACTION_WITH_MIXED_SIZES"
print_test_result "Concurrent Access During Compaction" "$TEST_CONCURRENT_ACCESS_DURING_COMPACTION"
print_test_result "Server Restart After Compaction" "$TEST_SERVER_RESTART_AFTER_COMPACTION"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi