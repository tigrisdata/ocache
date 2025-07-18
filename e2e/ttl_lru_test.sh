#!/bin/bash

echo "=== OCache TTL and LRU Eviction E2E Test ==="
echo

# Start the server with specific settings
echo "Starting OCache server with:"
echo "  - TTL cleanup: Enabled (automatic)"
echo "  - Max disk usage: 10KB (with LRU eviction)"
echo

./ocache \
  -disk /tmp/ocache-demo \
  -threshold 100 \
  -max-disk-usage 10240 \
  -v &

SERVER_PID=$!
sleep 2

echo "Server started with PID: $SERVER_PID"
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
echo "Waiting 70 seconds for TTL cleanup to run (cleanup runs every minute)..."
sleep 70

echo
echo "Listing keys after TTL cleanup (expired keys should be gone):"
./ocachecli list

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
echo "Waiting for eviction to run..."
sleep 6

echo
echo "Listing remaining keys (oldest should be evicted):"
./ocachecli list | grep "lru-key"

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

echo
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

# Track test results
TEST_PASSED=true

# Verify TTL test results
echo
echo "=== Test Results ==="
echo -n "Test 1 (TTL Cleanup): "
if ./ocachecli list | grep -q "ttl-key"; then
    echo "FAILED - TTL keys still exist"
    TEST_PASSED=false
else
    echo "PASSED"
fi

# Verify LRU test results
echo -n "Test 2 (LRU Eviction): "
RECENT_EXISTS=true
OLD_EVICTED=true

# Check if recent keys exist
for i in {18..20}; do
    if ! ./ocachecli get "lru-key-$i" > /dev/null 2>&1; then
        RECENT_EXISTS=false
    fi
done

# Check if old keys were evicted
for i in {1..5}; do
    if ./ocachecli get "lru-key-$i" > /dev/null 2>&1; then
        OLD_EVICTED=false
    fi
done

if [ "$RECENT_EXISTS" = true ] && [ "$OLD_EVICTED" = true ]; then
    echo "PASSED"
else
    echo "FAILED"
    [ "$RECENT_EXISTS" = false ] && echo "  - Recent keys were incorrectly evicted"
    [ "$OLD_EVICTED" = false ] && echo "  - Old keys were not evicted"
    TEST_PASSED=false
fi

# Cleanup
rm -rf /tmp/ocache-demo

# Exit with appropriate code
if [ "$TEST_PASSED" = true ]; then
    echo
    echo "All tests passed!"
    exit 0
else
    echo
    echo "Some tests failed!"
    exit 1
fi