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
    
    # Clean up test directories
    for dir in "${CLEANUP_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            rm -rf "$dir"
        fi
    done
    
    # Clean up any temp files
    rm -f /tmp/ocache-*-errors-* /tmp/keys-*.txt
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
