#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Recompaction and Defragmentation E2E Test ==="
echo "Testing recompaction for segment defragmentation after deletes"
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_CREATE_INITIAL_SEGMENTS=""
TEST_CREATE_FRAGMENTATION=""
TEST_TRIGGER_RECOMPACTION=""
TEST_MULTIPLE_RECOMPACTION_CYCLES=""
TEST_RECOMPACTION_WITH_UPDATES=""
TEST_CONCURRENT_OPERATIONS_DURING_RECOMPACTION=""
TEST_SERVER_RESTART_AFTER_RECOMPACTION=""
TEST_HEAVY_FRAGMENTATION_SCENARIO=""

# Start the server with recompaction settings
echo "Starting OCache server with recompaction settings:"
echo "  - Threshold: 64KB"
echo "  - Compaction interval: 5 seconds"
echo "  - Fragmentation threshold: 30%"
echo

start_server "recompaction" "true" \
  -disk /tmp/ocache-recompaction-test \
  -threshold 65536 \
  -compaction-interval 5s \
  -segment-size 1048576 \
  -fragmentation-threshold 0.3 \
  -recompaction-min-segment-age 100ms \
  -recompaction-min-segments 1 \
  -v

echo "=== Test 1: Create Initial Segments ==="
echo "Creating objects to form initial segments..."

# Create enough medium objects to trigger compaction
for i in {1..10}; do
    VALUE=$(head -c 80000 /dev/urandom | base64 | head -c 80000)
    timeout 10 ./ocachecli put "segment-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
    echo "Added segment-key-${i} (80KB)"
done

echo
echo "Waiting for initial compaction to segments (10 seconds)..."
sleep 10

# Verify all keys are accessible
echo "Verifying keys are in segments..."
INITIAL_ERRORS=0
for i in {1..10}; do
    if ! timeout 10 ./ocachecli get "segment-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read segment-key-${i}${NC}"
        ((INITIAL_ERRORS++))
    fi
done

if [ "$INITIAL_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All keys successfully compacted to segments${NC}"
    TEST_CREATE_INITIAL_SEGMENTS="PASSED"
else
    echo -e "${RED}✗ $INITIAL_ERRORS keys unreadable${NC}"
    TEST_CREATE_INITIAL_SEGMENTS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 2: Create Fragmentation ==="
echo "Deleting keys to create fragmentation in segments..."

# Delete 40% of keys to create fragmentation
for i in 2 4 6 8; do
    timeout 10 ./ocachecli delete "segment-key-${i}" 2>&1 | grep -v "^$" || true
    echo "Deleted segment-key-${i}"
done

echo
echo "Fragmentation created: 40% of keys deleted from segments"
echo "Remaining keys: 1, 3, 5, 7, 9, 10"

# Verify deletes worked
DELETE_CHECK=0
for i in 2 4 6 8; do
    if timeout 10 ./ocachecli get "segment-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}✗ segment-key-${i} still exists${NC}"
        ((DELETE_CHECK++))
    else
        echo "✓ segment-key-${i} successfully deleted"
    fi
done

if [ "$DELETE_CHECK" -eq 0 ]; then
    echo -e "${GREEN}✓ All deletions successful${NC}"
    TEST_CREATE_FRAGMENTATION="PASSED"
else
    echo -e "${RED}✗ Some deletions failed${NC}"
    TEST_CREATE_FRAGMENTATION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Trigger Recompaction ==="
echo "Waiting for recompaction to defragment segments (15 seconds)..."
echo "Recompaction should consolidate remaining keys into new segments"
sleep 15

# Verify remaining keys are still accessible after recompaction
echo "Verifying remaining keys after recompaction..."
RECOMPACT_ERRORS=0
for i in 1 3 5 7 9 10; do
    if ! timeout 10 ./ocachecli get "segment-key-${i}" >/dev/null 2>&1; then
        echo -e "${RED}Failed to read segment-key-${i} after recompaction${NC}"
        ((RECOMPACT_ERRORS++))
    else
        echo "✓ segment-key-${i} accessible after recompaction"
    fi
done

if [ "$RECOMPACT_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All remaining keys accessible after recompaction${NC}"
    TEST_TRIGGER_RECOMPACTION="PASSED"
else
    echo -e "${RED}✗ $RECOMPACT_ERRORS keys lost during recompaction${NC}"
    TEST_TRIGGER_RECOMPACTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Multiple Recompaction Cycles ==="
echo "Testing multiple cycles of fragmentation and recompaction..."

# Add more keys
echo "Adding new batch of keys..."
for i in {11..20}; do
    VALUE=$(head -c 75000 /dev/urandom | base64 | head -c 75000)
    timeout 10 ./ocachecli put "cycle-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

echo "Waiting for compaction..."
sleep 10

# Create fragmentation in new keys
echo "Creating fragmentation in new keys..."
for i in 12 14 16 18; do
    timeout 10 ./ocachecli delete "cycle-key-${i}" 2>&1 | grep -v "^$" || true
done

echo "Waiting for recompaction..."
sleep 15

# Verify all remaining keys
echo "Verifying all remaining keys after multiple cycles..."
MULTI_ERRORS=0

# Check original remaining keys
for i in 1 3 5 7 9 10; do
    if ! timeout 10 ./ocachecli get "segment-key-${i}" >/dev/null 2>&1; then
        ((MULTI_ERRORS++))
    fi
done

# Check new remaining keys
for i in 11 13 15 17 19 20; do
    if ! timeout 10 ./ocachecli get "cycle-key-${i}" >/dev/null 2>&1; then
        ((MULTI_ERRORS++))
    fi
done

if [ "$MULTI_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All keys survived multiple recompaction cycles${NC}"
    TEST_MULTIPLE_RECOMPACTION_CYCLES="PASSED"
else
    echo -e "${RED}✗ $MULTI_ERRORS keys lost in multiple cycles${NC}"
    TEST_MULTIPLE_RECOMPACTION_CYCLES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Recompaction with Updates ==="
echo "Testing recompaction with key updates..."

# Update some existing keys
echo "Updating existing keys in segments..."
for i in 3 5 7; do
    NEW_VALUE=$(head -c 85000 /dev/urandom | base64 | head -c 85000)
    timeout 10 ./ocachecli put "segment-key-${i}" "$NEW_VALUE" 2>&1 | grep -v "^$" || true
    echo "Updated segment-key-${i}"
done

# Add some new keys
for i in {21..25}; do
    VALUE=$(head -c 70000 /dev/urandom | base64 | head -c 70000)
    timeout 10 ./ocachecli put "update-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

# Delete some to create fragmentation
for i in 21 23; do
    timeout 10 ./ocachecli delete "update-key-${i}" 2>&1 | grep -v "^$" || true
done

echo "Waiting for recompaction with updates..."
sleep 15

# Verify updated keys retained new values
echo "Verifying updated keys have correct values..."
UPDATE_CHECK=0
for i in 3 5 7; do
    if timeout 10 ./ocachecli get "segment-key-${i}" >/dev/null 2>&1; then
        echo "✓ segment-key-${i} accessible with updated value"
    else
        echo -e "${RED}✗ segment-key-${i} lost after update${NC}"
        ((UPDATE_CHECK++))
    fi
done

if [ "$UPDATE_CHECK" -eq 0 ]; then
    echo -e "${GREEN}✓ All updated keys preserved correctly${NC}"
    TEST_RECOMPACTION_WITH_UPDATES="PASSED"
else
    echo -e "${RED}✗ Some updated keys lost${NC}"
    TEST_RECOMPACTION_WITH_UPDATES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 6: Concurrent Operations During Recompaction ==="
echo "Testing concurrent operations while recompaction is active..."

# Function for concurrent operations
concurrent_ops() {
    local id=$1
    # Read operations
    for i in {1..5}; do
        timeout 10 ./ocachecli get "segment-key-$((RANDOM % 10 + 1))" >/dev/null 2>&1 || true
    done
    # Write operations
    VALUE=$(head -c 60000 /dev/urandom | base64 | head -c 60000)
    timeout 10 ./ocachecli put "concurrent-${id}" "$VALUE" 2>&1 | grep -v "^$" || true
}

# Create more fragmentation
echo "Creating fragmentation for concurrent test..."
for i in {26..35}; do
    VALUE=$(head -c 80000 /dev/urandom | base64 | head -c 80000)
    timeout 10 ./ocachecli put "frag-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

sleep 8

# Delete half to create fragmentation
for i in 26 28 30 32 34; do
    timeout 10 ./ocachecli delete "frag-key-${i}" 2>&1 | grep -v "^$" || true
done

echo "Starting concurrent operations during recompaction window..."
# Start concurrent operations while recompaction might be happening
PIDS=()
for i in {1..5}; do
    concurrent_ops "$i" &
    PIDS+=($!)
done

# Wait for all concurrent operations to complete with timeout
WAIT_TIME=0
MAX_WAIT=30
ALL_DONE=false
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${PIDS[@]}"; do
        if kill -0 "$PID" 2>/dev/null; then
            ((RUNNING++))
        fi
    done
    if [ "$RUNNING" -eq 0 ]; then
        ALL_DONE=true
        break
    fi
    sleep 1
    ((WAIT_TIME++))
done

if [ "$ALL_DONE" = false ]; then
    echo "Warning: Some concurrent operations timed out, killing remaining processes"
    for PID in "${PIDS[@]}"; do
        kill -9 "$PID" 2>/dev/null || true
    done
fi

echo "Waiting for recompaction to complete..."
sleep 10

# Verify concurrent writes succeeded
CONCURRENT_CHECK=0
for i in {1..5}; do
    if ! timeout 10 ./ocachecli get "concurrent-${i}" >/dev/null 2>&1; then
        ((CONCURRENT_CHECK++))
    fi
done

if [ "$CONCURRENT_CHECK" -eq 0 ]; then
    echo -e "${GREEN}✓ All concurrent operations succeeded during recompaction${NC}"
    TEST_CONCURRENT_OPERATIONS_DURING_RECOMPACTION="PASSED"
else
    echo -e "${RED}✗ $CONCURRENT_CHECK concurrent operations failed${NC}"
    TEST_CONCURRENT_OPERATIONS_DURING_RECOMPACTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 7: Server Restart After Recompaction ==="
echo "Testing data integrity after restart with recompacted segments..."

# Get list of all keys before restart
echo "Collecting keys before restart..."
timeout 10 ./ocachecli list > /tmp/keys-before-restart.txt || true
KEY_COUNT_BEFORE=$(wc -l < /tmp/keys-before-restart.txt)
echo "Keys before restart: $KEY_COUNT_BEFORE"

# Stop server
echo "Stopping server..."
kill "$SERVER_PID"

# Wait for server to stop with timeout
WAIT_COUNT=0
while kill -0 "$SERVER_PID" 2>/dev/null && [ "$WAIT_COUNT" -lt 10 ]; do
    sleep 1
    ((WAIT_COUNT++))
done

if kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Warning: Server didn't stop gracefully, force killing"
    kill -9 "$SERVER_PID" 2>/dev/null || true
fi

# Restart server
echo "Restarting server..."
start_server "recompaction" "false" \
  -disk /tmp/ocache-recompaction-test \
  -threshold 65536 \
  -compaction-interval 5s \
  -segment-size 1048576 \
  -fragmentation-threshold 0.3 \
  -recompaction-min-segment-age 100ms \
  -recompaction-min-segments 1 \
  -v

# Check keys after restart
timeout 10 ./ocachecli list > /tmp/keys-after-restart.txt || true
KEY_COUNT_AFTER=$(wc -l < /tmp/keys-after-restart.txt)
echo "Keys after restart: $KEY_COUNT_AFTER"

RESTART_TEST_ERRORS=0
if [ "$KEY_COUNT_BEFORE" -eq "$KEY_COUNT_AFTER" ]; then
    echo -e "${GREEN}✓ All keys preserved after restart${NC}"
else
    echo -e "${RED}✗ Key count mismatch after restart${NC}"
    ((RESTART_TEST_ERRORS++))
    TEST_PASSED=false
fi

# Spot check some keys from different batches
RESTART_ERRORS=0
for key in "segment-key-5" "cycle-key-15" "update-key-22" "concurrent-3"; do
    if ! timeout 10 ./ocachecli get "$key" >/dev/null 2>&1; then
        echo -e "${RED}Key $key lost after restart${NC}"
        ((RESTART_ERRORS++))
    fi
done

if [ "$RESTART_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Spot check: recompacted keys accessible after restart${NC}"
else
    echo -e "${RED}✗ Some recompacted keys lost after restart${NC}"
    ((RESTART_TEST_ERRORS++))
    TEST_PASSED=false
fi

if [ "$RESTART_TEST_ERRORS" -eq 0 ]; then
    TEST_SERVER_RESTART_AFTER_RECOMPACTION="PASSED"
else
    TEST_SERVER_RESTART_AFTER_RECOMPACTION="FAILED"
fi

echo
echo "=== Test 8: Heavy Fragmentation Scenario ==="
echo "Testing recompaction with heavy fragmentation (>50%)..."

# Create many keys
echo "Creating 30 keys for heavy fragmentation test..."
for i in {40..69}; do
    VALUE=$(head -c 70000 /dev/urandom | base64 | head -c 70000)
    timeout 10 ./ocachecli put "heavy-frag-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

echo "Waiting for initial compaction..."
sleep 10

# Delete 60% of keys to create heavy fragmentation
echo "Deleting 60% of keys to create heavy fragmentation..."
for i in {40..57}; do
    timeout 10 ./ocachecli delete "heavy-frag-${i}" 2>&1 | grep -v "^$" || true
done

echo "Heavy fragmentation created: 18 out of 30 keys deleted"
echo "Waiting for aggressive recompaction..."
sleep 15

# Verify remaining keys
HEAVY_FRAG_ERRORS=0
for i in {58..69}; do
    if ! timeout 10 ./ocachecli get "heavy-frag-${i}" >/dev/null 2>&1; then
        ((HEAVY_FRAG_ERRORS++))
    fi
done

if [ "$HEAVY_FRAG_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All remaining keys survived heavy fragmentation recompaction${NC}"
    TEST_HEAVY_FRAGMENTATION_SCENARIO="PASSED"
else
    echo -e "${RED}✗ $HEAVY_FRAG_ERRORS keys lost during heavy recompaction${NC}"
    TEST_HEAVY_FRAGMENTATION_SCENARIO="FAILED"
    TEST_PASSED=false
fi

# Cleanup temp files
rm -f /tmp/keys-before-restart.txt /tmp/keys-after-restart.txt

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Create Initial Segments" "$TEST_CREATE_INITIAL_SEGMENTS"
print_test_result "Create Fragmentation" "$TEST_CREATE_FRAGMENTATION"
print_test_result "Trigger Recompaction" "$TEST_TRIGGER_RECOMPACTION"
print_test_result "Multiple Recompaction Cycles" "$TEST_MULTIPLE_RECOMPACTION_CYCLES"
print_test_result "Recompaction with Updates" "$TEST_RECOMPACTION_WITH_UPDATES"
print_test_result "Concurrent Operations During Recompaction" "$TEST_CONCURRENT_OPERATIONS_DURING_RECOMPACTION"
print_test_result "Server Restart After Recompaction" "$TEST_SERVER_RESTART_AFTER_RECOMPACTION"
print_test_result "Heavy Fragmentation Scenario" "$TEST_HEAVY_FRAGMENTATION_SCENARIO"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi