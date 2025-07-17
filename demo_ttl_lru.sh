#!/bin/bash

echo "=== OCache TTL and LRU Eviction Demo ==="
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

# Demo 1: TTL Cleanup
echo "=== Demo 1: TTL Cleanup ==="
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

# Demo 2: LRU Eviction
echo
echo "=== Demo 2: LRU Eviction ==="
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

echo "Demo complete!"
echo
echo "Summary of new features:"
echo "1. Background TTL cleanup runs periodically (configurable interval)"
echo "2. LRU eviction automatically frees space when disk usage limit is reached"
echo "3. Recently accessed keys are preserved during eviction"

# Cleanup
rm -rf /tmp/ocache-demo