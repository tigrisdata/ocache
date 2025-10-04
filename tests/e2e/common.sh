#!/usr/bin/env bash

# =============================================================================
# OCache E2E Test Common Functions Library
# =============================================================================
# This file contains common functionality used across all E2E test scripts.
# Source this file in your test scripts with: source "$(dirname "$0")/common.sh"

# =============================================================================
# Color Definitions
# =============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m' 
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# =============================================================================
# Global Variables
# =============================================================================
TEST_PASSED=true
SERVER_PID=""
CLUSTER_PIDS=()
CLEANUP_DIRS=()

# =============================================================================
# Binary and Environment Checks
# =============================================================================

# Check if required binaries exist
check_binaries() {
    if [ ! -f "./ocache" ] || [ ! -f "./ocachecli" ]; then
        echo -e "${RED}ERROR: ocache or ocachecli binary not found${NC}"
        echo "Please run 'make all' first"
        exit 1
    fi
}

# =============================================================================
# Server Management Functions
# =============================================================================

# Start OCache server with given parameters
# Usage: start_server <test_name> <server_args...>
start_server() {
    local test_name="$1"
    local cleanup="$2"
    
    shift 2
    local server_args="$*"
    
    local test_dir="/tmp/ocache-${test_name}-test"
    CLEANUP_DIRS+=("$test_dir")
    
    echo "Starting OCache server for ${test_name} test..."
    
    # Cleanup any previous test data
    if [ "$cleanup" = "true" ]; then
        rm -rf "$test_dir"
    fi
    
    # Start server
    ./ocache $server_args &
    SERVER_PID=$!
    sleep 2
    
    # Check if server started successfully
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "${RED}ERROR: Server failed to start${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Server started with PID: $SERVER_PID${NC}"
    echo
    
    # Test connectivity with timeout
    if ! timeout 10 ./ocachecli list >/dev/null 2>&1; then
        echo -e "${RED}ERROR: Cannot connect to server${NC}"
        stop_server
        exit 1
    fi
}

# Stop OCache server gracefully with timeout
stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 $SERVER_PID 2>/dev/null; then
        echo "Stopping server..."
        kill $SERVER_PID
        
        # Wait for server to stop with timeout
        local wait_count=0
        while kill -0 $SERVER_PID 2>/dev/null && [ $wait_count -lt 10 ]; do
            sleep 1
            ((wait_count++))
        done
        
        if kill -0 $SERVER_PID 2>/dev/null; then
            echo "Warning: Server didn't stop gracefully, force killing"
            kill -9 $SERVER_PID 2>/dev/null || true
        fi
    fi
    SERVER_PID=""
}

# Restart server with same arguments
# Usage: restart_server <test_name> <server_args...>
restart_server() {
    local test_name="$1"
    shift
    local server_args="$*"

    stop_server
    start_server "$test_name" $server_args
}

# =============================================================================
# Cluster Management Functions
# =============================================================================

# Start a cluster of OCache nodes
# Usage: start_cluster <test_name> <num_nodes> <base_grpc_port> <base_cluster_port> <cleanup> [additional_server_args...]
start_cluster() {
    local test_name="$1"
    local num_nodes="$2"
    local base_grpc_port="$3"
    local base_cluster_port="$4"
    local cleanup="$5"
    shift 5
    local additional_args="$*"

    local test_dir="/tmp/ocache-${test_name}-test"
    CLEANUP_DIRS+=("$test_dir")

    echo "Starting ${num_nodes}-node OCache cluster for ${test_name} test..."

    # Cleanup any previous test data
    if [ "$cleanup" = "true" ]; then
        rm -rf "$test_dir"
    fi

    # Build seeds list for all nodes
    local seeds=""
    for i in $(seq 1 "$num_nodes"); do
        local cluster_port=$((base_cluster_port + i - 1))
        if [ -n "$seeds" ]; then
            seeds="${seeds},localhost:${cluster_port}"
        else
            seeds="localhost:${cluster_port}"
        fi
    done

    # Start each node
    CLUSTER_PIDS=()
    for i in $(seq 1 "$num_nodes"); do
        local node_id="node${i}"
        local grpc_port=$((base_grpc_port + i - 1))
        local http_port=$((grpc_port + 100))
        local cluster_port=$((base_cluster_port + i - 1))
        local node_disk="${test_dir}/${node_id}"

        mkdir -p "$node_disk"

        echo "  Starting ${node_id} on gRPC port ${grpc_port}, HTTP port ${http_port}, cluster port ${cluster_port}..."

        ./ocache \
            -cluster-enabled \
            -node-id "$node_id" \
            -listen-addr ":${grpc_port}" \
            -listen-http ":${http_port}" \
            -cluster-addr ":${cluster_port}" \
            -seeds "$seeds" \
            -disk "$node_disk" \
            $additional_args &

        local pid=$!
        CLUSTER_PIDS+=($pid)

        sleep 1

        # Check if node started successfully
        if ! kill -0 $pid 2>/dev/null; then
            echo -e "${RED}ERROR: Node ${node_id} failed to start${NC}"
            stop_cluster
            exit 1
        fi

        echo -e "${GREEN}  Node ${node_id} started with PID: $pid${NC}"
    done

    echo
    echo "Waiting for cluster to stabilize..."
    sleep 5

    # Test connectivity to first node with timeout
    local first_port=$((base_grpc_port))
    if ! timeout 10 ./ocachecli --addr "localhost:${first_port}" list >/dev/null 2>&1; then
        echo -e "${RED}ERROR: Cannot connect to cluster${NC}"
        stop_cluster
        exit 1
    fi

    echo -e "${GREEN}Cluster started successfully with ${num_nodes} nodes${NC}"
    echo
}

# Stop all cluster nodes gracefully with timeout
stop_cluster() {
    if [ ${#CLUSTER_PIDS[@]} -eq 0 ]; then
        return
    fi

    echo "Stopping cluster nodes..."

    for pid in "${CLUSTER_PIDS[@]}"; do
        if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
            kill $pid 2>/dev/null || true
        fi
    done

    # Wait for all nodes to stop with timeout
    local wait_count=0
    local all_stopped=false
    while [ $wait_count -lt 10 ]; do
        local running=0
        for pid in "${CLUSTER_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                ((running++))
            fi
        done

        if [ $running -eq 0 ]; then
            all_stopped=true
            break
        fi

        sleep 1
        ((wait_count++))
    done

    if [ "$all_stopped" = false ]; then
        echo "Warning: Some nodes didn't stop gracefully, force killing"
        for pid in "${CLUSTER_PIDS[@]}"; do
            kill -9 $pid 2>/dev/null || true
        done
    fi

    CLUSTER_PIDS=()
}

# Get cluster addresses as comma-separated string
# Usage: get_cluster_addrs <base_grpc_port> <num_nodes>
get_cluster_addrs() {
    local base_port="$1"
    local num_nodes="$2"

    local addrs=""
    for i in $(seq 1 "$num_nodes"); do
        local port=$((base_port + i - 1))
        if [ -n "$addrs" ]; then
            addrs="${addrs},localhost:${port}"
        else
            addrs="localhost:${port}"
        fi
    done

    echo "$addrs"
}

# =============================================================================
# Test Result Management
# =============================================================================

# Mark a test as failed
# Usage: fail_test <test_var_name> <error_message>
fail_test() {
    local test_var="$1"
    local message="$2"
    
    eval "${test_var}=FAILED"
    TEST_PASSED=false
    echo -e "${RED}✗ ${message}${NC}"
}

# Mark a test as passed
# Usage: pass_test <test_var_name> <success_message>
pass_test() {
    local test_var="$1"
    local message="$2"
    
    eval "${test_var}=PASSED"
    echo -e "${GREEN}✓ ${message}${NC}"
}

# Print test result with proper formatting
# Usage: print_test_result <test_description> <test_var_name>
print_test_result() {
    local description="$1"
    local result="$2"
    
    if [ -n "$result" ]; then
        case "$result" in
            "PASSED")
                echo -e "✓ ${description}: ${GREEN}PASSED${NC}"
                ;;
            "FAILED")
                echo -e "✗ ${description}: ${RED}FAILED${NC}"
                ;;
            "WARNING")
                echo -e "⚠ ${description}: ${YELLOW}WARNING${NC}"
                ;;
            *)
                echo -e "? ${description}: ${result}"
                ;;
        esac
    fi
}

# Print overall test results summary
# Usage: print_overall_result
print_overall_result() {
    echo
    if [ "$TEST_PASSED" = true ]; then
        echo -e "${GREEN}Overall Result: All tests PASSED!${NC}"
    else
        echo -e "${RED}Overall Result: Some tests FAILED!${NC}"
    fi
}

# =============================================================================
# Background Process Management
# =============================================================================

# Wait for background processes to complete with timeout
# Usage: wait_for_processes <max_wait_seconds> <process_pids_array...>
wait_for_processes() {
    local max_wait="$1"
    shift
    local pids=("$@")
    
    local wait_time=0
    local all_done=false
    
    echo "Waiting for background processes to complete (max ${max_wait}s)..."
    
    while [ "$wait_time" -lt "$max_wait" ]; do
        local running=0
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                ((running++))
            fi
        done
        
        if [ "$running" -eq 0 ]; then
            all_done=true
            break
        fi
        
        sleep 1
        ((wait_time++))
        
        # Progress indicator every 5 seconds
        if [ $((wait_time % 5)) -eq 0 ]; then
            echo "  Still waiting... ($running processes running)"
        fi
    done
    
    if [ "$all_done" = false ]; then
        echo -e "${YELLOW}⚠ Timeout: Killing remaining processes${NC}"
        for pid in "${pids[@]}"; do
            kill -9 "$pid" 2>/dev/null || true
        done
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

# Generate random data of specified size
# Usage: generate_random_data <size_in_bytes>
generate_random_data() {
    local size="$1"
    head -c "$size" /dev/urandom | base64 | head -c "$size"
}

# Generate checksum for data (using md5 for portability)
# Usage: generate_checksum <data>
generate_checksum() {
    local data="$1"
    if command -v md5sum >/dev/null 2>&1; then
        printf "%s" "$data" | md5sum | awk '{print $1}'
    elif command -v md5 >/dev/null 2>&1; then
        printf "%s" "$data" | md5 -r | awk '{print $1}'
    else
        echo "ERROR: Neither md5sum nor md5 found"
        return 1
    fi
}

# Verify data matches expected checksum
# Usage: verify_checksum <data> <expected_checksum>
verify_checksum() {
    local data="$1"
    local expected="$2"
    local actual
    
    actual=$(generate_checksum "$data")
    if [ "$actual" = "$expected" ]; then
        return 0
    else
        echo "Checksum mismatch: expected $expected, got $actual"
        return 1
    fi
}

# Store test data with checksum for later verification
# Usage: store_test_data <key> <data> <checksum_file>
store_test_data() {
    local key="$1"
    local data="$2"
    local checksum_file="$3"
    
    local checksum
    checksum=$(generate_checksum "$data")
    
    # Remove any existing entry for this key before adding the new one
    if [ -f "$checksum_file" ]; then
        grep -v "^${key}:" "$checksum_file" > "${checksum_file}.tmp" 2>/dev/null || true
        mv "${checksum_file}.tmp" "$checksum_file"
    fi
    
    # Add the new checksum
    echo "${key}:${checksum}" >> "$checksum_file"
}

# Verify stored data integrity
# Usage: verify_stored_data <key> <checksum_file>
verify_stored_data() {
    local key="$1"
    local checksum_file="$2"
    
    # Get expected checksum
    local expected_checksum
    expected_checksum=$(grep "^${key}:" "$checksum_file" 2>/dev/null | cut -d: -f2)
    
    if [ -z "$expected_checksum" ]; then
        echo "No checksum found for key: $key"
        return 1
    fi
    
    # Get actual data
    local actual_data
    actual_data=$(./ocachecli get "$key" 2>/dev/null)
    
    if [ -z "$actual_data" ]; then
        echo "Failed to retrieve data for key: $key"
        return 1
    fi
    
    # Verify checksum
    if verify_checksum "$actual_data" "$expected_checksum"; then
        return 0
    else
        echo "Data integrity check failed for key: $key"
        return 1
    fi
}

# Generate deterministic test data (for reproducible testing)
# Usage: generate_deterministic_data <seed> <size>
generate_deterministic_data() {
    local seed="$1"
    local size="$2"
    
    # Use seed to generate reproducible data
    yes "${seed}" | head -c "$size" | base64 | head -c "$size"
}

# Verify data content matches expected value
# Usage: verify_data_content <key> <expected_value>
verify_data_content() {
    local key="$1"
    local expected="$2"
    
    local actual
    actual=$(./ocachecli get "$key" 2>/dev/null)
    
    if [ "$actual" = "$expected" ]; then
        return 0
    else
        echo "Content mismatch for key $key"
        echo "Expected: ${expected:0:50}..." 
        echo "Actual: ${actual:0:50}..."
        return 1
    fi
}

# Execute ocachecli command with timeout and error handling
# Usage: ocache_cmd <timeout_seconds> <command> [args...]
ocache_cmd() {
    local timeout_sec="$1"
    shift
    timeout "$timeout_sec" ./ocachecli "$@" 2>&1 || true
}

# Check if a key exists
# Usage: key_exists <key_name>
key_exists() {
    local key="$1"
    timeout 10 ./ocachecli get "$key" >/dev/null 2>&1
}

# Delete key with error suppression
# Usage: delete_key <key_name>
delete_key() {
    local key="$1"
    timeout 10 ./ocachecli delete "$key" 2>&1 | grep -v "Key not found" || true
}

# =============================================================================
# Cleanup Functions
# =============================================================================

# Cleanup function to be called on exit
cleanup_common() {
    echo
    echo "Cleaning up..."

    # Stop server if running
    stop_server

    # Stop cluster if running
    stop_cluster

    # Clean up test directories
    for dir in "${CLEANUP_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            rm -rf "$dir"
        fi
    done

    # Clean up any temp files
    rm -f /tmp/ocache-*-errors-* /tmp/keys-*.txt /tmp/checksums-*.txt
}

# Set up trap to cleanup on exit
setup_cleanup_trap() {
    trap cleanup_common EXIT INT TERM
}

# =============================================================================
# Test Pattern Functions
# =============================================================================

# Run a set of concurrent operations with timeout
# Usage: run_concurrent_operations <timeout> <function_name> <num_workers> [additional_args...]
run_concurrent_operations() {
    local timeout="$1"
    local func_name="$2"
    local num_workers="$3"
    shift 3
    local additional_args="$*"
    
    local pids=()
    
    echo "Starting $num_workers concurrent workers..."
    for i in $(seq 1 "$num_workers"); do
        "$func_name" "$i" $additional_args &
        pids+=($!)
    done
    
    wait_for_processes "$timeout" "${pids[@]}"
}

# Verify multiple keys exist
# Usage: verify_keys_exist <key_prefix> <start_num> <end_num>
verify_keys_exist() {
    local prefix="$1"
    local start="$2" 
    local end="$3"
    
    local errors=0
    for i in $(seq "$start" "$end"); do
        if ! key_exists "${prefix}${i}"; then
            echo -e "${RED}Key ${prefix}${i} not found${NC}"
            ((errors++))
        fi
    done
    
    return $errors
}

# =============================================================================
# Initialization
# =============================================================================

# Initialize common library (call this early in your script)
init_common() {
    check_binaries
    setup_cleanup_trap
}

# =============================================================================
# Example Usage
# =============================================================================
# 
# In your test script:
# 
# #!/usr/bin/env bash
# source "$(dirname "$0")/common.sh"
# 
# init_common
# 
# # Your test variables
# TEST_EXAMPLE_FEATURE=""
# 
# # Start server
# start_server "example" -disk /tmp/ocache-example-test -threshold 64000
# 
# # Run tests...
# if some_test_condition; then
#     pass_test "TEST_EXAMPLE_FEATURE" "Example feature works correctly"
# else
#     fail_test "TEST_EXAMPLE_FEATURE" "Example feature failed"
# fi
# 
# # Print results
# echo "=== Test Results Summary ==="
# print_test_result "Example Feature" "TEST_EXAMPLE_FEATURE"
# print_overall_result
# 
# # Exit (cleanup will be called automatically)
# exit $([ "$TEST_PASSED" = true ] && echo 0 || echo 1)
