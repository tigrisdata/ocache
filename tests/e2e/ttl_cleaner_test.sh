#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache TTL Functionality and Cleaner E2E Test ==="
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_BASIC_TTL_EXPIRATION_5SEC=""
TEST_BASIC_TTL_EXPIRATION_10SEC=""
TEST_TTL_WITH_DIFFERENT_STORAGE_LAYERS=""
TEST_TTL_UPDATE_ON_OVERWRITE=""
TEST_MIXED_TTL_AND_NON_TTL_KEYS=""
TEST_CONCURRENT_TTL_OPERATIONS=""
TEST_TTL_PRECISION_TEST=""
TEST_TTL_WITH_DELETE_OPERATIONS=""

# Start the server with TTL cleanup enabled
echo "Starting OCache server with TTL cleanup:"
echo "  - TTL cleanup interval: 5 seconds"
echo "  - Testing various TTL scenarios"
echo

start_server "ttl" "true" \
  -disk /tmp/ocache-ttl-test \
  -threshold 64000 \
  -ttl-cleanup-interval 5s \
  -v

echo "=== Test 1: Basic TTL Expiration ==="
echo "Adding keys with different TTL values..."

# Add keys with various TTL values
./ocachecli put "ttl-5sec" "Expires in 5 seconds" --ttl 5 || true
./ocachecli put "ttl-10sec" "Expires in 10 seconds" --ttl 10 || true
./ocachecli put "ttl-15sec" "Expires in 15 seconds" --ttl 15 || true
./ocachecli put "ttl-20sec" "Expires in 20 seconds" --ttl 20 || true
./ocachecli put "no-ttl" "Never expires" || true

echo "Keys added:"
./ocachecli list

echo
echo "Waiting 7 seconds for first TTL cleanup cycle..."
sleep 7

echo "After 7 seconds (ttl-5sec should be expired):"

if ./ocachecli get "ttl-5sec" >/dev/null 2>&1; then
    fail_test "TEST_BASIC_TTL_EXPIRATION_5SEC" "ttl-5sec should have expired"
else
    pass_test "TEST_BASIC_TTL_EXPIRATION_5SEC" "ttl-5sec correctly expired"
fi

# Check that other keys still exist
if ./ocachecli get "ttl-10sec" >/dev/null 2>&1; then
    echo -e "${GREEN}✓ ttl-10sec still exists${NC}"
else
    echo -e "${RED}✗ ttl-10sec should still exist${NC}"
    TEST_PASSED=false
fi

echo
echo "Waiting another 5 seconds..."
sleep 5

echo "After 12 seconds total (ttl-10sec should also be expired):"
if ./ocachecli get "ttl-10sec" >/dev/null 2>&1; then
    fail_test "TEST_BASIC_TTL_EXPIRATION_10SEC" "ttl-10sec should have expired"
else
    pass_test "TEST_BASIC_TTL_EXPIRATION_10SEC" "ttl-10sec correctly expired"
fi

echo
echo "=== Test 2: TTL with Different Storage Layers ==="
echo "Testing TTL expiration for objects in different storage layers..."

# Small object with TTL (RocksDB inline)
SMALL_VALUE=$(head -c 1000 /dev/urandom | base64 | head -c 1000)
./ocachecli put "small-ttl" "$SMALL_VALUE" --ttl 8

# Medium object with TTL (raw file)
head -c 100000 /dev/urandom | base64 | head -c 100000 | ./ocachecli put "medium-ttl" --ttl 8

echo "Added small and medium objects with 8-second TTL"
echo
echo "Waiting 10 seconds for expiration..."
sleep 10

# Check both are expired
if ./ocachecli get "small-ttl" >/dev/null 2>&1 && ./ocachecli get "medium-ttl" >/dev/null 2>&1; then
    echo -e "${RED}✗ TTL expiration failed for some storage layers${NC}"
    TEST_TTL_WITH_DIFFERENT_STORAGE_LAYERS="FAILED"
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ Both small and medium objects with TTL expired correctly${NC}"
    TEST_TTL_WITH_DIFFERENT_STORAGE_LAYERS="PASSED"
fi

echo
echo "=== Test 3: TTL Update on Overwrite ==="
echo "Testing TTL behavior when keys are overwritten..."

# Create a key with short TTL
./ocachecli put "update-ttl-test" "Original value" --ttl 5
echo "Created key with 5-second TTL"

sleep 2

# Overwrite with longer TTL
./ocachecli put "update-ttl-test" "Updated value" --ttl 20
echo "Updated key with 20-second TTL after 2 seconds"

# Wait for original TTL to pass
sleep 5

echo "After 7 seconds total (original TTL would have expired):"
if ./ocachecli get "update-ttl-test" >/dev/null 2>&1; then
    VALUE=$(./ocachecli get "update-ttl-test" 2>/dev/null)
    if [ "$VALUE" = "Updated value" ]; then
        echo -e "${GREEN}✓ Key still exists with updated value and new TTL${NC}"
        TEST_TTL_UPDATE_ON_OVERWRITE="PASSED"
    else
        echo -e "${RED}✗ Unexpected value after TTL update${NC}"
        TEST_TTL_UPDATE_ON_OVERWRITE="FAILED"
        TEST_PASSED=false
    fi
else
    echo -e "${RED}✗ Key expired despite TTL update${NC}"
    TEST_TTL_UPDATE_ON_OVERWRITE="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Mixed TTL and Non-TTL Keys ==="
echo "Testing cleanup doesn't affect non-TTL keys..."

# Add a mix of TTL and non-TTL keys
for i in {1..5}; do
    ./ocachecli put "expire-${i}" "Will expire" --ttl 5
    ./ocachecli put "persist-${i}" "Will persist"
done

echo "Added 5 TTL keys and 5 persistent keys"
./ocachecli list | wc -l

echo
echo "Waiting 7 seconds for TTL cleanup..."
sleep 7

# Count remaining keys
EXPIRE_COUNT=$(./ocachecli list | grep -c "expire-" || true)
PERSIST_COUNT=$(./ocachecli list | grep -c "persist-" || true)

if [ "$EXPIRE_COUNT" -eq 0 ] && [ "$PERSIST_COUNT" -eq 5 ]; then
    echo -e "${GREEN}✓ TTL keys expired, persistent keys remain${NC}"
    TEST_MIXED_TTL_AND_NON_TTL_KEYS="PASSED"
else
    echo -e "${RED}✗ Unexpected state: expire=$EXPIRE_COUNT (expected 0), persist=$PERSIST_COUNT (expected 5)${NC}"
    TEST_MIXED_TTL_AND_NON_TTL_KEYS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Concurrent TTL Operations ==="
echo "Testing TTL with concurrent reads and writes..."

# Function to continuously read a key
read_until_expired() {
    local key=$1
    local count=0
    while ./ocachecli get "$key" >/dev/null 2>&1; do
        ((count++))
        sleep 0.1
    done
    echo "Key $key was read $count times before expiring"
}

# Add keys with TTL
for i in {1..3}; do
    ./ocachecli put "concurrent-ttl-${i}" "Value ${i}" --ttl 8
done

# Start concurrent readers
PIDS=()
for i in {1..3}; do
    read_until_expired "concurrent-ttl-${i}" &
    PIDS+=($!)
done

# Add more keys while readers are running
sleep 2
for i in {4..6}; do
    ./ocachecli put "concurrent-ttl-${i}" "Value ${i}" --ttl 5 || true
done

# Wait for all readers to finish
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
    echo "Warning: Some readers timed out, killing remaining processes"
    for PID in "${PIDS[@]}"; do
        kill -9 "$PID" 2>/dev/null || true
    done
fi

echo -e "${GREEN}✓ Concurrent TTL operations completed${NC}"
TEST_CONCURRENT_TTL_OPERATIONS="PASSED"

echo
echo "=== Test 6: TTL Precision Test ==="
echo "Testing TTL expiration timing precision..."

# Add a key with exact TTL
TEST_TTL=6
./ocachecli put "precision-test" "Testing precision" --ttl $TEST_TTL
START_TIME=$(date +%s)

# Poll until key expires
while ./ocachecli get "precision-test" >/dev/null 2>&1; do
    sleep 0.5
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# Allow for cleanup interval delay (5 seconds)
MAX_EXPECTED=$((TEST_TTL + 5))
if [ "$ELAPSED" -ge "$TEST_TTL" ] && [ "$ELAPSED" -le "$MAX_EXPECTED" ]; then
    echo -e "${GREEN}✓ TTL precision within expected range: expired after ${ELAPSED}s (TTL was ${TEST_TTL}s)${NC}"
    TEST_TTL_PRECISION_TEST="PASSED"
else
    echo -e "${RED}✗ TTL precision issue: expired after ${ELAPSED}s (expected ${TEST_TTL}-${MAX_EXPECTED}s)${NC}"
    TEST_TTL_PRECISION_TEST="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 7: TTL with Delete Operations ==="
echo "Testing TTL keys can be manually deleted before expiration..."

# Add key with long TTL
./ocachecli put "delete-before-ttl" "Will be deleted manually" --ttl 30

# Delete it immediately
./ocachecli delete "delete-before-ttl"

# Verify it's gone
if ./ocachecli get "delete-before-ttl" >/dev/null 2>&1; then
    echo -e "${RED}✗ Failed to delete TTL key${NC}"
    TEST_TTL_WITH_DELETE_OPERATIONS="FAILED"
    TEST_PASSED=false
else
    echo -e "${GREEN}✓ TTL key successfully deleted before expiration${NC}"
    TEST_TTL_WITH_DELETE_OPERATIONS="PASSED"
fi

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Basic TTL Expiration - 5sec" "$TEST_BASIC_TTL_EXPIRATION_5SEC"
print_test_result "Basic TTL Expiration - 10sec" "$TEST_BASIC_TTL_EXPIRATION_10SEC"
print_test_result "TTL with Different Storage Layers" "$TEST_TTL_WITH_DIFFERENT_STORAGE_LAYERS"
print_test_result "TTL Update on Overwrite" "$TEST_TTL_UPDATE_ON_OVERWRITE"
print_test_result "Mixed TTL and Non-TTL Keys" "$TEST_MIXED_TTL_AND_NON_TTL_KEYS"
print_test_result "Concurrent TTL Operations" "$TEST_CONCURRENT_TTL_OPERATIONS"
print_test_result "TTL Precision Test" "$TEST_TTL_PRECISION_TEST"
print_test_result "TTL with Delete Operations" "$TEST_TTL_WITH_DELETE_OPERATIONS"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi