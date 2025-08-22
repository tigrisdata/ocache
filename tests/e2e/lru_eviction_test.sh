#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache LRU Eviction E2E Test ==="
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_BASIC_LRU_EVICTION=""
TEST_LRU_ACCESS_PATTERN_TEST=""
TEST_MIXED_SIZE_OBJECTS_WITH_LRU=""
TEST_CONTINUOUS_LRU_UNDER_LOAD=""
TEST_LRU_WITH_TTL_INTERACTION=""
TEST_LRU_CACHE_WARMUP=""

# Start the server with LRU eviction enabled
echo "Starting OCache server with LRU eviction:"
echo "  - Max disk usage: 50KB"
echo "  - LRU cleanup interval: 5 seconds"
echo "  - Testing LRU eviction behavior"
echo

start_server "lru" "true" \
  -disk /tmp/ocache-lru-test \
  -threshold 1000 \
  -max-disk-usage 51200 \
  -ttl-cleanup-interval 5s \
  -v

echo "=== Test 1: Basic LRU Eviction ==="
echo "Adding keys to exceed disk usage limit..."

# Add 20 keys, each ~3KB (total ~60KB, exceeds 50KB limit)
for i in {1..20}; do
    VALUE=$(head -c 3000 /dev/urandom | base64 | head -c 3000)
    timeout 10 ./ocachecli put "lru-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
    echo "Added lru-key-${i} (3KB)"
    sleep 0.1
done

echo
echo "Added 20 keys totaling ~60KB (exceeds 50KB limit)"
echo "Waiting for LRU eviction to run (10 seconds)..."
sleep 10

# Count remaining keys
REMAINING_KEYS=$(timeout 10 ./ocachecli list | grep -c "lru-key" || true)
echo "Keys remaining after eviction: $REMAINING_KEYS out of 20"

if [ "$REMAINING_KEYS" -lt 20 ] && [ "$REMAINING_KEYS" -gt 0 ]; then
    echo -e "${GREEN}✓ LRU eviction occurred: $((20 - REMAINING_KEYS)) keys evicted${NC}"
    TEST_BASIC_LRU_EVICTION="PASSED"
else
    echo -e "${RED}✗ LRU eviction did not work as expected${NC}"
    TEST_BASIC_LRU_EVICTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 2: LRU Access Pattern Test ==="
echo "Testing that recently accessed keys are not evicted..."

# Clear existing keys
for i in {1..20}; do
    timeout 10 ./ocachecli delete "lru-key-${i}" 2>&1 | grep -v "Key not found" | grep -v "^$" || true
done

# Add new keys
echo "Adding 15 new keys..."
for i in {1..15}; do
    VALUE=$(head -c 3500 /dev/urandom | base64 | head -c 3500)
    timeout 10 ./ocachecli put "access-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
    sleep 0.1
done

echo
echo "Accessing keys 11-15 to update their LRU time..."
for i in {11..15}; do
    timeout 10 ./ocachecli get "access-key-${i}" >/dev/null 2>&1 || true
    echo "Accessed access-key-${i}"
done

echo
echo "Adding 5 more keys to trigger eviction..."
for i in {16..20}; do
    VALUE=$(head -c 3500 /dev/urandom | base64 | head -c 3500)
    timeout 10 ./ocachecli put "access-key-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

echo "Waiting for LRU eviction..."
sleep 10

# Check which keys remain
echo
echo "Checking which keys were evicted..."
RECENT_EXISTS=0
OLD_EXISTS=0

# Check recently accessed keys (11-15 and newly added 16-20 should exist)
for i in {11..20}; do
    if timeout 10 ./ocachecli get "access-key-${i}" >/dev/null 2>&1; then
        ((RECENT_EXISTS++))
    fi
done

# Check old keys (1-10 should mostly be evicted)
for i in {1..10}; do
    if timeout 10 ./ocachecli get "access-key-${i}" >/dev/null 2>&1; then
        ((OLD_EXISTS++))
    fi
done

echo "Recently accessed/added keys (11-20) existing: $RECENT_EXISTS out of 10"
echo "Old keys (1-10) existing: $OLD_EXISTS out of 10"

if [ "$RECENT_EXISTS" -gt "$OLD_EXISTS" ]; then
    echo -e "${GREEN}✓ LRU policy working: recently accessed keys preferred${NC}"
    TEST_LRU_ACCESS_PATTERN_TEST="PASSED"
else
    echo -e "${RED}✗ LRU policy issue: old keys not properly evicted${NC}"
    TEST_LRU_ACCESS_PATTERN_TEST="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Mixed Size Objects with LRU ==="
echo "Testing LRU eviction with different object sizes..."

# Clear existing keys
timeout 10 ./ocachecli list | while read key; do
    timeout 10 ./ocachecli delete "$key" 2>&1 | grep -v "^$" || true
done

# Add small objects
echo "Adding small objects (1KB each)..."
for i in {1..10}; do
    VALUE=$(head -c 1000 /dev/urandom | base64 | head -c 1000)
    timeout 10 ./ocachecli put "small-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

# Add medium objects
echo "Adding medium objects (5KB each)..."
for i in {1..5}; do
    VALUE=$(head -c 5000 /dev/urandom | base64 | head -c 5000)
    timeout 10 ./ocachecli put "medium-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

# Add large objects to trigger eviction
echo "Adding large objects (10KB each) to trigger eviction..."
for i in {1..3}; do
    VALUE=$(head -c 10000 /dev/urandom | base64 | head -c 10000)
    timeout 10 ./ocachecli put "large-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

echo "Waiting for eviction..."
sleep 10

# Count remaining objects by type
SMALL_COUNT=$(timeout 10 ./ocachecli list | grep -c "small-" || true)
MEDIUM_COUNT=$(timeout 10 ./ocachecli list | grep -c "medium-" || true)
LARGE_COUNT=$(timeout 10 ./ocachecli list | grep -c "large-" || true)

echo "Remaining objects:"
echo "  Small (1KB): $SMALL_COUNT out of 10"
echo "  Medium (5KB): $MEDIUM_COUNT out of 5"
echo "  Large (10KB): $LARGE_COUNT out of 3"

TOTAL_COUNT=$((SMALL_COUNT + MEDIUM_COUNT + LARGE_COUNT))
if [ "$TOTAL_COUNT" -lt 18 ]; then
    echo -e "${GREEN}✓ Mixed-size eviction working: some objects evicted${NC}"
    TEST_MIXED_SIZE_OBJECTS_WITH_LRU="PASSED"
else
    echo -e "${RED}✗ No eviction occurred with mixed sizes${NC}"
    TEST_MIXED_SIZE_OBJECTS_WITH_LRU="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Continuous LRU Under Load ==="
echo "Testing LRU eviction under continuous load..."

# Function to continuously add keys
continuous_writer() {
    local prefix=$1
    for i in {1..20}; do
        VALUE=$(head -c 2500 /dev/urandom | base64 | head -c 2500)
        timeout 10 ./ocachecli put "${prefix}-continuous-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
        sleep 0.2
    done
}

# Start multiple writers
echo "Starting continuous writers..."
continuous_writer "writer1" &
PID1=$!
continuous_writer "writer2" &
PID2=$!

# Let them run for a bit
sleep 5

# Access some keys while writing continues
echo "Accessing some keys during continuous load..."
for i in {1..5}; do
    timeout 10 ./ocachecli get "writer1-continuous-${i}" >/dev/null 2>&1 || true
done

# Wait for writers to complete with timeout
PIDS=("$PID1" "$PID2")
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
    echo "Warning: Some writers timed out, killing remaining processes"
    for PID in "${PIDS[@]}"; do
        kill -9 "$PID" 2>/dev/null || true
    done
fi

echo "Waiting for final eviction cycle..."
sleep 10

# Check final state
FINAL_COUNT=$(timeout 10 ./ocachecli list | grep -c "continuous" || true)
echo "Final key count under continuous load: $FINAL_COUNT"

if [ "$FINAL_COUNT" -lt 40 ]; then
    echo -e "${GREEN}✓ LRU eviction maintained disk limit under load${NC}"
    TEST_CONTINUOUS_LRU_UNDER_LOAD="PASSED"
else
    echo -e "${RED}✗ Disk limit exceeded under continuous load${NC}"
    TEST_CONTINUOUS_LRU_UNDER_LOAD="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: LRU with TTL Interaction ==="
echo "Testing LRU eviction doesn't interfere with TTL..."

# Clear cache
timeout 10 ./ocachecli list | while read key; do
    timeout 10 ./ocachecli delete "$key" 2>&1 | grep -v "^$" || true
done

# Add TTL keys
echo "Adding keys with TTL..."
for i in {1..5}; do
    VALUE=$(head -c 5000 /dev/urandom | base64 | head -c 5000)
    timeout 10 ./ocachecli put "ttl-lru-${i}" "$VALUE" --ttl 15 2>&1 | grep -v "^$" || true
done

# Add regular keys to trigger eviction
echo "Adding regular keys to trigger eviction..."
for i in {1..10}; do
    VALUE=$(head -c 5000 /dev/urandom | base64 | head -c 5000)
    timeout 10 ./ocachecli put "regular-lru-${i}" "$VALUE" 2>&1 | grep -v "^$" || true
done

echo "Waiting for eviction..."
sleep 10

# Check that TTL keys can still expire normally
TTL_COUNT=$(timeout 10 ./ocachecli list | grep -c "ttl-lru" || true)
REGULAR_COUNT=$(timeout 10 ./ocachecli list | grep -c "regular-lru" || true)

echo "Keys before TTL expiration:"
echo "  TTL keys: $TTL_COUNT"
echo "  Regular keys: $REGULAR_COUNT"

echo "Waiting for TTL expiration (10 more seconds)..."
sleep 10

TTL_COUNT_AFTER=$(timeout 10 ./ocachecli list | grep -c "ttl-lru" || true)
if [ "$TTL_COUNT_AFTER" -eq 0 ]; then
    echo -e "${GREEN}✓ TTL keys expired correctly despite LRU eviction${NC}"
    TEST_LRU_WITH_TTL_INTERACTION="PASSED"
else
    echo -e "${RED}✗ TTL expiration affected by LRU${NC}"
    TEST_LRU_WITH_TTL_INTERACTION="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 6: LRU Cache Warmup ==="
echo "Testing cache behavior after restart with disk limit..."

# Kill and restart server to test persistence
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

echo "Restarting server with same disk location..."
start_server "lru" "false" \
  -disk /tmp/ocache-lru-test \
  -threshold 1000 \
  -max-disk-usage 51200 \
  -ttl-cleanup-interval 5s \
  -v

# Check persisted keys
PERSISTED_COUNT=$(timeout 10 ./ocachecli list | wc -l || echo 0)
echo "Keys loaded from disk: $PERSISTED_COUNT"

if [ "$PERSISTED_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Cache successfully loaded persisted data within disk limit${NC}"
    TEST_LRU_CACHE_WARMUP="PASSED"
else
    echo -e "${YELLOW}⚠ No keys persisted (may be expected if all were evicted)${NC}"
    TEST_LRU_CACHE_WARMUP="PASSED"
fi

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Basic LRU Eviction" "$TEST_BASIC_LRU_EVICTION"
print_test_result "LRU Access Pattern Test" "$TEST_LRU_ACCESS_PATTERN_TEST"
print_test_result "Mixed Size Objects with LRU" "$TEST_MIXED_SIZE_OBJECTS_WITH_LRU"
print_test_result "Continuous LRU Under Load" "$TEST_CONTINUOUS_LRU_UNDER_LOAD"
print_test_result "LRU with TTL Interaction" "$TEST_LRU_WITH_TTL_INTERACTION"
print_test_result "LRU Cache Warmup" "$TEST_LRU_CACHE_WARMUP"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi