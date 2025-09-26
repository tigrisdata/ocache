#!/bin/bash

# Test script to verify cluster address separation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting 3-node cluster with separate cluster and listen addresses...${NC}"

# Clean up any existing processes
pkill -f "ocache.*cluster-enabled" || true
sleep 2

# Create temp directories for each node
TEMP_DIR=$(mktemp -d)
mkdir -p "${TEMP_DIR}/node1" "${TEMP_DIR}/node2" "${TEMP_DIR}/node3"

# Start node 1
echo -e "${GREEN}Starting node1 (cluster: 7001, listen: 9001)...${NC}"
./ocache -cluster-enabled -node-id node1 -disk "${TEMP_DIR}/node1" \
    -listen-addr :9001 -cluster-addr :7001 \
    -seeds localhost:7002,localhost:7003 \
    > "${TEMP_DIR}/node1.log" 2>&1 &
NODE1_PID=$!

sleep 2

# Start node 2
echo -e "${GREEN}Starting node2 (cluster: 7002, listen: 9002)...${NC}"
./ocache -cluster-enabled -node-id node2 -disk "${TEMP_DIR}/node2" \
    -listen-addr :9002 -cluster-addr :7002 \
    -seeds localhost:7001,localhost:7003 \
    > "${TEMP_DIR}/node2.log" 2>&1 &
NODE2_PID=$!

sleep 2

# Start node 3
echo -e "${GREEN}Starting node3 (cluster: 7003, listen: 9003)...${NC}"
./ocache -cluster-enabled -node-id node3 -disk "${TEMP_DIR}/node3" \
    -listen-addr :9003 -cluster-addr :7003 \
    -seeds localhost:7001,localhost:7002 \
    > "${TEMP_DIR}/node3.log" 2>&1 &
NODE3_PID=$!

echo -e "${YELLOW}Waiting for cluster to stabilize...${NC}"
sleep 5

# Test 1: Put data via cluster client
echo -e "${YELLOW}Test 1: Writing data through cluster client...${NC}"
./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" put test-key "test-value"

# Test 2: Get data from different node
echo -e "${YELLOW}Test 2: Reading data from different node...${NC}"
VALUE=$(./ocachecli --addr "localhost:9002" get test-key)
if [[ "$VALUE" == "test-value" ]]; then
    echo -e "${GREEN}✓ Data successfully retrieved from different node${NC}"
else
    echo -e "${RED}✗ Failed to retrieve data. Got: $VALUE${NC}"
fi

# Test 3: Verify routing works correctly
echo -e "${YELLOW}Test 3: Testing multiple keys to verify routing...${NC}"
for i in {1..10}; do
    ./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" put "key-$i" "value-$i"
done

# Retrieve all keys
FAILED=0
for i in {1..10}; do
    VALUE=$(./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" get "key-$i")
    if [[ "$VALUE" != "value-$i" ]]; then
        echo -e "${RED}✗ Failed to retrieve key-$i${NC}"
        FAILED=1
    fi
done

if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}✓ All keys successfully retrieved through cluster routing${NC}"
fi

# Check logs for errors
echo -e "${YELLOW}Checking logs for routing errors...${NC}"
if grep -q "routing error\|failed to connect to remote node" "${TEMP_DIR}"/*.log; then
    echo -e "${RED}✗ Found routing errors in logs:${NC}"
    grep "routing error\|failed to connect to remote node" "${TEMP_DIR}"/*.log
else
    echo -e "${GREEN}✓ No routing errors found in logs${NC}"
fi

# Check that nodes are using correct addresses
echo -e "${YELLOW}Verifying address usage in logs...${NC}"
if grep -q "cluster_address.*:700[1-3].*listen_address.*:900[1-3]" "${TEMP_DIR}"/*.log; then
    echo -e "${GREEN}✓ Nodes are correctly using separate cluster and listen addresses${NC}"
else
    echo -e "${YELLOW}! Could not verify address separation from logs${NC}"
fi

# Cleanup
echo -e "${YELLOW}Cleaning up...${NC}"
kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
rm -rf "$TEMP_DIR"

echo -e "${GREEN}Test completed!${NC}"