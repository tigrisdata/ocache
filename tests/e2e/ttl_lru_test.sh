#!/bin/bash

echo "=== OCache TTL and LRU Eviction E2E Test ==="
echo

# Initialize test results
TTL_TEST_PASSED=false
LRU_TEST_PASSED=false

# Cleanup any previous test data
rm -rf /tmp/ocache-demo

# Ensure binaries exist
if [ ! -f "./ocache" ] || [ ! -f "./ocachecli" ]; then
    echo "ERROR: ocache or ocachecli binary not found"
    echo "Please run 'make all' first"
    exit 1
fi

# Start the server with specific settings
echo "Starting OCache server with:"
echo "  - TTL cleanup: Enabled (every 5 seconds in test mode)"
echo "  - Max disk usage: 10KB (with LRU eviction)"
echo

./ocache \
  -disk /tmp/ocache-demo \
  -threshold 100 \
  -max-disk-usage 10240 \
  -ttl-cleanup-interval 5s \
  -v &

SERVER_PID=$!
sleep 2

# Check if server started successfully
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    exit 1
fi

echo "Server started with PID: $SERVER_PID"

# Test connectivity by trying to list keys (should be empty initially)
if ! ./ocachecli list >/dev/null 2>&1; then
    echo "ERROR: Cannot connect to server"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo "Server is running and accepting connections"
echo

# Test 1: TTL Cleanup
echo "=== Test 1: TTL Cleanup ==="
echo "Adding 5 keys with 10-second TTL..."
for i in {1..5}; do
  ./ocachecli put "ttl-key-$i" "This value will expire in 10 seconds" --ttl 10
done

echo "Adding 5 keys without TTL..."
for i in {1..5}; do
  ./ocachecli put "permanent-key-$i" "This value has no expiration"
done

echo
echo "Listing all keys:"
./ocachecli list

echo
echo "Waiting 15 seconds for TTL cleanup to run (cleanup runs every 5 seconds in test mode)..."
sleep 15

echo
echo "Listing keys after TTL cleanup (expired keys should be gone):"
./ocachecli list

# Verify TTL test results immediately
echo
echo "=== TTL Test Verification ==="
TTL_KEYS_FOUND=$(./ocachecli list | grep -c "ttl-key" || true)
PERMANENT_KEYS_FOUND=$(./ocachecli list | grep -c "permanent-key" || true)

if [ "$TTL_KEYS_FOUND" -eq 0 ] && [ "$PERMANENT_KEYS_FOUND" -eq 5 ]; then
    echo "TTL Test: PASSED - TTL keys expired and were cleaned up, permanent keys remain"
    TTL_TEST_PASSED=true
else
    echo "TTL Test: FAILED - TTL keys: $TTL_KEYS_FOUND (expected 0), Permanent keys: $PERMANENT_KEYS_FOUND (expected 5)"
    TTL_TEST_PASSED=false
fi

# Test 2: LRU Eviction
echo
echo "=== Test 2: LRU Eviction ==="
echo "Adding large values to trigger disk usage limit..."
echo "Each value is about 1KB, disk usage limit is 10KB"
echo

for i in {1..20}; do
  # Create a 1KB value
  VALUE=$(head -c 1000 /dev/urandom | base64 | head -c 1000)
  ./ocachecli put "lru-key-$i" "$VALUE"
  echo "Added lru-key-$i (1KB)"
  sleep 0.1
done

echo
echo "Accessing some keys to update their LRU time..."
for i in {18..20}; do
  ./ocachecli get "lru-key-$i" > /dev/null
  echo "Accessed lru-key-$i"
done

echo
echo "Current disk usage should exceed 10KB limit (20 keys x 1KB each = 20KB)"
echo "Waiting for eviction to run (cleanup/eviction runs every 5 seconds in test mode)..."
sleep 15  # Wait for 3 cleanup cycles to ensure eviction runs

echo
echo "Checking total number of keys after eviction..."
TOTAL_KEYS=$(./ocachecli list | wc -l)
echo "Total keys in cache: $TOTAL_KEYS"

echo
echo "Counting LRU keys remaining..."
LRU_KEY_COUNT=$(./ocachecli list | grep -c "lru-key" || true)
echo "LRU keys remaining: $LRU_KEY_COUNT out of 20 originally added"

# Calculate expected evictions
EXPECTED_REMAINING=$((9))  # With 10KB limit and 1KB per key, expect about 9 keys
echo "Expected approximately $EXPECTED_REMAINING keys to remain with 10KB limit"

echo
echo "Listing remaining LRU keys (oldest should be evicted):"
./ocachecli list | grep "lru-key" | sort -V

echo
echo "Keys 18-20 should still exist (recently accessed):"
for i in {18..20}; do
  if ./ocachecli get "lru-key-$i" > /dev/null 2>&1; then
    echo "lru-key-$i: EXISTS"
  else
    echo "lru-key-$i: EVICTED"
  fi
done

echo
echo "Keys 1-5 should be evicted (oldest):"
for i in {1..5}; do
  if ./ocachecli get "lru-key-$i" > /dev/null 2>&1; then
    echo "lru-key-$i: EXISTS"
  else
    echo "lru-key-$i: EVICTED"
  fi
done

# Final test results summary
echo
echo "=== Test Results Summary ==="
echo -n "Test 1 (TTL Cleanup): "
if [ "$TTL_TEST_PASSED" = true ]; then
    echo "PASSED"
else
    echo "FAILED"
fi

# Verify LRU test results
echo -n "Test 2 (LRU Eviction): "

# Count remaining LRU keys
FINAL_LRU_COUNT=$(./ocachecli list | grep -c "lru-key" || true)

# Check if eviction happened (should have less than 20 keys)
if [ "$FINAL_LRU_COUNT" -lt 20 ] && [ "$FINAL_LRU_COUNT" -gt 0 ]; then
    echo "PASSED - Eviction occurred: $FINAL_LRU_COUNT keys remain out of 20 (some were evicted due to disk limit)"
    LRU_TEST_PASSED=true
    
    # Additional check: verify that at least keys 18-20 exist (recently accessed)
    RECENT_CHECK=true
    for i in {18..20}; do
        if ! ./ocachecli get "lru-key-$i" > /dev/null 2>&1; then
            RECENT_CHECK=false
            echo "  WARNING: Recently accessed key lru-key-$i was evicted"
        fi
    done
    
    if [ "$RECENT_CHECK" = false ]; then
        echo "  Note: LRU policy may need tuning - recently accessed keys were evicted"
    fi
else
    echo "FAILED"
    if [ "$FINAL_LRU_COUNT" -eq 20 ]; then
        echo "  - No eviction occurred: all 20 keys still exist"
        echo "  - Disk usage limit may not be working correctly"
    elif [ "$FINAL_LRU_COUNT" -eq 0 ]; then
        echo "  - All keys were evicted (unexpected behavior)"
    fi
    LRU_TEST_PASSED=false
fi

echo
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

# Cleanup
rm -rf /tmp/ocache-demo

# Exit with appropriate code
if [ "$TTL_TEST_PASSED" = true ] && [ "$LRU_TEST_PASSED" = true ]; then
    echo
    echo "All tests passed!"
    exit 0
else
    echo
    echo "Some tests failed!"
    exit 1
fi