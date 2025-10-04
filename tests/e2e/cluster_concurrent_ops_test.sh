#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Cluster Mode Concurrent Operations E2E Test ==="
echo

# Initialize common functionality
init_common

# Use regular variables instead of associative array for compatibility
TEST_CONCURRENT_WRITES=""
TEST_CONCURRENT_READS=""
TEST_MIXED_OPS_COUNT=""
TEST_MIXED_OPS_VALUES=""
TEST_READ_AFTER_WRITE=""
TEST_CONCURRENT_DELETES=""

# Cluster configuration
NUM_NODES=3
BASE_GRPC_PORT=9001
BASE_CLUSTER_PORT=7001

# Start the cluster
echo "Starting ${NUM_NODES}-node OCache cluster..."
start_cluster "cluster-concurrent" "$NUM_NODES" "$BASE_GRPC_PORT" "$BASE_CLUSTER_PORT" "true" \
  -threshold 64000 \
  -ttl-cleanup-interval 10s \
  -v

# Get cluster addresses for CLI
CLUSTER_ADDRS=$(get_cluster_addrs "$BASE_GRPC_PORT" "$NUM_NODES")
echo "Using cluster addresses: $CLUSTER_ADDRS"
echo

# Helper function to run ocachecli with cluster addresses
cluster_cli() {
    ./ocachecli --addr "$CLUSTER_ADDRS" "$@"
}

# Helper function to count keys across all cluster nodes
cluster_count_keys() {
    local pattern="$1"
    local total=0

    # Query each node individually and sum the results
    for i in $(seq 1 "$NUM_NODES"); do
        local port=$((BASE_GRPC_PORT + i - 1))
        local count=0
        local keys
        keys=$(./ocachecli --mode simple --addr "localhost:${port}" list 2>/dev/null || echo "")

        # Only count non-empty lines
        if [ -n "$pattern" ]; then
            count=$(echo "$keys" | grep -c "$pattern")
        else
            count=$(echo "$keys" | wc -l)
        fi

        # Strip whitespace and ensure it's a number
        count=${count:-0}
        total=$((total + count))
    done

    echo "$total"
}

echo "=== Test 1: Concurrent Writes ==="
echo "Writing 100 keys concurrently from 10 processes..."

# Function to write keys in background
write_keys() {
    local prefix=$1
    local count=$2
    local errors=0
    local success=0
    for i in $(seq 1 "$count"); do
        if ! cluster_cli put "${prefix}-key-${i}" "value-${prefix}-${i}"; then
            ((errors++))
        else
            ((success++))
        fi
    done

    echo "Writer $prefix had $success successes and $errors errors"
}

# Start concurrent writers
echo "Starting 10 concurrent writer processes..."
PIDS=()
for writer in {1..10}; do
    write_keys "writer${writer}" 10 &
    PIDS+=($!)
done

# Wait for all writers to complete
echo "Waiting for writers to complete..."
WAIT_TIME=0
MAX_WAIT=60
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
    if [ $((WAIT_TIME % 5)) -eq 0 ]; then
        echo "  Still waiting... ($RUNNING processes running)"
    fi
done

# Give cluster time to stabilize
sleep 2

if [ "$ALL_DONE" = false ]; then
    echo -e "${YELLOW}⚠ Timeout: Killing remaining writer processes${NC}"
    for PID in "${PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

echo "Verifying all 100 keys were written across all cluster nodes..."
KEY_COUNT=$(cluster_count_keys "")
if [ "$KEY_COUNT" -eq 100 ]; then
    echo -e "${GREEN}✓ All 100 keys written successfully across cluster${NC}"
    TEST_CONCURRENT_WRITES="PASSED"
else
    echo -e "${RED}✗ Expected 100 keys, found $KEY_COUNT${NC}"
    TEST_CONCURRENT_WRITES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 2: Concurrent Reads with Consistency Check ==="
echo "Reading all keys concurrently and verifying values..."

# Function to read and verify keys
read_verify_keys() {
    local prefix=$1
    local count=$2
    local errors=0
    for i in $(seq 1 "$count"); do
        local key="${prefix}-key-${i}"
        local expected="value-${prefix}-${i}"
        local actual
        if actual=$(cluster_cli get "$key" 2>/dev/null); then
            if [ "$actual" != "$expected" ]; then
                echo -e "${RED}Mismatch for $key: expected '$expected', got '$actual'${NC}"
                ((errors++))
            fi
        else
            echo -e "${RED}Failed to read $key${NC}"
            ((errors++))
        fi
    done
    echo $errors
}

# Start concurrent readers
echo "Starting 10 concurrent reader processes..."
TOTAL_ERRORS=0
READER_PIDS=()
for reader in {1..10}; do
    (
        ERROR_COUNT=$(read_verify_keys "writer${reader}" 10)
        echo "$ERROR_COUNT" > "/tmp/ocache-cluster-concurrent-test-errors-${reader}"
    ) &
    READER_PIDS+=($!)
done

# Wait for all readers
echo "Waiting for readers to complete..."
WAIT_TIME=0
MAX_WAIT=30
ALL_DONE=false
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${READER_PIDS[@]}"; do
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
    if [ $((WAIT_TIME % 5)) -eq 0 ]; then
        echo "  Still waiting... ($RUNNING readers running)"
    fi
done

if [ "$ALL_DONE" = false ]; then
    echo -e "${YELLOW}⚠ Timeout: Killing remaining reader processes${NC}"
    for PID in "${READER_PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

# Collect errors
for reader in {1..10}; do
    if [ -f "/tmp/ocache-cluster-concurrent-test-errors-${reader}" ]; then
        if ERRORS=$(cat "/tmp/ocache-cluster-concurrent-test-errors-${reader}" 2>/dev/null); then
            TOTAL_ERRORS=$((TOTAL_ERRORS + ERRORS))
        fi
        rm -f "/tmp/ocache-cluster-concurrent-test-errors-${reader}"
    fi
done

if [ "$TOTAL_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All reads returned correct values from cluster${NC}"
    TEST_CONCURRENT_READS="PASSED"
else
    echo -e "${RED}✗ Found $TOTAL_ERRORS read errors${NC}"
    TEST_CONCURRENT_READS="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 3: Concurrent Mixed Operations ==="
echo "Running concurrent puts, gets, and deletes..."

# Function for mixed operations
mixed_ops() {
    local id=$1
    local base="mixed${id}"

    # Put some keys
    for i in {1..5}; do
        cluster_cli put "${base}-key-${i}" "value-${i}" 2>&1 || true
    done

    # Read some keys
    for i in {1..3}; do
        cluster_cli get "${base}-key-${i}" >/dev/null 2>&1 || true
    done

    # Delete some keys
    for i in {1..2}; do
        cluster_cli delete "${base}-key-${i}" 2>&1 || true
    done

    # Update remaining keys
    for i in {3..5}; do
        cluster_cli put "${base}-key-${i}" "updated-value-${i}" 2>&1 || true
    done
}

# Run mixed operations concurrently
echo "Starting 5 concurrent mixed operation workers..."
MIXED_PIDS=()
for worker in {1..5}; do
    mixed_ops "$worker" &
    MIXED_PIDS+=($!)
done

# Wait
WAIT_TIME=0
MAX_WAIT=30
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${MIXED_PIDS[@]}"; do
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
        echo "  Still waiting... ($RUNNING workers running)"
    fi
done

if [ "$RUNNING" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Killing remaining mixed ops processes${NC}"
    for PID in "${MIXED_PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

echo "Verifying final state across all cluster nodes..."

# Each worker should have keys 3,4,5 remaining (1,2 were deleted)
EXPECTED_KEYS=$((5 * 3))  # 5 workers * 3 remaining keys each
MIXED_COUNT=$(cluster_count_keys "^mixed")

if [ "$MIXED_COUNT" -eq "$EXPECTED_KEYS" ]; then
    echo -e "${GREEN}✓ Correct number of keys after mixed operations: $MIXED_COUNT${NC}"
    TEST_MIXED_OPS_COUNT="PASSED"
else
    echo -e "${RED}✗ Expected $EXPECTED_KEYS keys, found $MIXED_COUNT${NC}"
    TEST_MIXED_OPS_COUNT="FAILED"
    TEST_PASSED=false
fi

# Verify updated values
echo "Verifying updated values..."
VERIFY_ERRORS=0
for worker in {1..5}; do
    for i in {3..5}; do
        KEY="mixed${worker}-key-${i}"
        EXPECTED="updated-value-${i}"
        if ACTUAL=$(cluster_cli get "$KEY" 2>/dev/null); then
            if [ "$ACTUAL" != "$EXPECTED" ]; then
                echo -e "${RED}Value mismatch for $KEY: got '$ACTUAL', expected '$EXPECTED'${NC}"
                ((VERIFY_ERRORS++))
            fi
        else
            echo -e "${RED}Failed to read $KEY${NC}"
            ((VERIFY_ERRORS++))
        fi
    done
done

if [ "$VERIFY_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ All updated values are correct in cluster${NC}"
    TEST_MIXED_OPS_VALUES="PASSED"
else
    echo -e "${RED}✗ Found $VERIFY_ERRORS value mismatches${NC}"
    TEST_MIXED_OPS_VALUES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 4: Read-After-Write Consistency ==="
echo "Testing immediate read after write from concurrent clients..."

# Function to test read-after-write
raw_test() {
    local id=$1
    local errors=0

    for i in {1..10}; do
        local key="raw-${id}-${i}"
        local value="consistency-test-${id}-${i}-${RANDOM}"

        # Write
        if cluster_cli put "$key" "$value" 2>&1; then
            # Immediate read
            local read_value
            if read_value=$(cluster_cli get "$key" 2>/dev/null); then
                if [ "$read_value" != "$value" ]; then
                    echo -e "${RED}Read-after-write failed for $key${NC}"
                    ((errors++))
                fi
            else
                echo -e "${RED}Failed to read $key after write${NC}"
                ((errors++))
            fi
        else
            echo -e "${RED}Failed to write $key${NC}"
            ((errors++))
        fi
    done

    echo $errors
}

# Run read-after-write tests concurrently
echo "Starting read-after-write tests..."
RAW_ERRORS=0
RAW_PIDS=()
for client in {1..5}; do
    (
        ERROR_COUNT=$(raw_test "$client")
        echo "$ERROR_COUNT" > "/tmp/ocache-cluster-raw-errors-${client}"
    ) &
    RAW_PIDS+=($!)
done

# Wait
WAIT_TIME=0
MAX_WAIT=30
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${RAW_PIDS[@]}"; do
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

if [ "$RUNNING" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Killing remaining RAW test processes${NC}"
    for PID in "${RAW_PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

# Collect errors
for client in {1..5}; do
    if [ -f "/tmp/ocache-cluster-raw-errors-${client}" ]; then
        if ERRORS=$(cat "/tmp/ocache-cluster-raw-errors-${client}" 2>/dev/null); then
            RAW_ERRORS=$((RAW_ERRORS + ERRORS))
        fi
        rm -f "/tmp/ocache-cluster-raw-errors-${client}"
    fi
done

if [ "$RAW_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}✓ Read-after-write consistency maintained in cluster${NC}"
    TEST_READ_AFTER_WRITE="PASSED"
else
    echo -e "${RED}✗ Found $RAW_ERRORS consistency violations${NC}"
    TEST_READ_AFTER_WRITE="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test 5: Concurrent Deletes ==="
echo "Testing concurrent deletion of same keys..."

# Create keys for deletion test
echo "Creating keys for deletion test..."
for i in {1..20}; do
    cluster_cli put "delete-test-${i}" "to-be-deleted-${i}" 2>&1 || true
done

# Function to delete keys
delete_keys() {
    local start=$1
    local end=$2
    for i in $(seq "$start" "$end"); do
        cluster_cli delete "delete-test-${i}" 2>&1 | grep -v "Key not found" || true
    done
}

# Concurrent overlapping deletes
echo "Running concurrent deletes..."
DELETE_PIDS=()
delete_keys 1 10 &
DELETE_PIDS+=($!)
delete_keys 5 15 &
DELETE_PIDS+=($!)
delete_keys 10 20 &
DELETE_PIDS+=($!)

# Wait
WAIT_TIME=0
MAX_WAIT=30
while [ "$WAIT_TIME" -lt "$MAX_WAIT" ]; do
    RUNNING=0
    for PID in "${DELETE_PIDS[@]}"; do
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

if [ "$RUNNING" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Killing remaining delete processes${NC}"
    for PID in "${DELETE_PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done
fi

# Verify all keys are deleted across all cluster nodes
REMAINING=$(cluster_count_keys "delete-test")
if [ "$REMAINING" -eq 0 ]; then
    echo -e "${GREEN}✓ All keys successfully deleted from cluster${NC}"
    TEST_CONCURRENT_DELETES="PASSED"
else
    echo -e "${RED}✗ $REMAINING keys still remain after deletion${NC}"
    TEST_CONCURRENT_DELETES="FAILED"
    TEST_PASSED=false
fi

echo
echo "=== Test Results Summary ==="
echo
echo "Individual Test Results:"
echo "------------------------"
# Print individual test results
print_test_result "Concurrent Writes" "$TEST_CONCURRENT_WRITES"
print_test_result "Concurrent Reads with Consistency" "$TEST_CONCURRENT_READS"
print_test_result "Mixed Operations - Key Count" "$TEST_MIXED_OPS_COUNT"
print_test_result "Mixed Operations - Value Updates" "$TEST_MIXED_OPS_VALUES"
print_test_result "Read-After-Write Consistency" "$TEST_READ_AFTER_WRITE"
print_test_result "Concurrent Deletes" "$TEST_CONCURRENT_DELETES"

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi
