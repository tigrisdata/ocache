#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Eviction E2E Test (LRU + FIFO) ==="
echo

# Initialize common functionality
init_common

# Per-(policy,test) results are stored in variables named RESULT_<policy>_<test>.
get_result() { eval "printf '%s' \"\${RESULT_${1}_${2}:-}\""; }

# clear_all_keys removes every key currently in the cache.
clear_all_keys() {
    ./ocachecli list 2>/dev/null | while read -r key; do
        [ -n "$key" ] && ./ocachecli delete "$key" >/dev/null 2>&1 || true
    done
}

# run_eviction_suite runs the eviction test suite against a single policy.
# Usage: run_eviction_suite <lru|fifo>
run_eviction_suite() {
    local policy="$1"
    local dir="/tmp/ocache-${policy}-eviction-test"

    echo
    echo "############################################################"
    echo "### Eviction suite: policy=${policy}"
    echo "############################################################"
    echo

    # Start a server with the given eviction policy. FIFO indexes keys as they
    # are written, so it must be enabled from a fresh deployment — always start
    # clean (cleanup=true) for both policies to keep the runs independent.
    start_server "${policy}-eviction" "true" \
      -disk "$dir" \
      -threshold 1000 \
      -max-disk-usage 51200 \
      -eviction-policy "$policy" \
      -ttl-cleanup-interval 5s \
      -v

    # ---------------------------------------------------------------------------
    # Test 1: Basic eviction (policy-agnostic)
    # ---------------------------------------------------------------------------
    echo "=== [${policy}] Test 1: Basic eviction ==="
    echo "Adding 20 keys (~3KB each, ~60KB total) to exceed the 50KB limit..."
    for i in $(seq 1 20); do
        VALUE=$(generate_random_data 3000)
        ./ocachecli put "evict-key-${i}" "$VALUE" >/dev/null 2>&1 || true
        sleep 0.1
    done

    echo "Waiting for eviction to run..."
    sleep 10

    REMAINING_KEYS=$(./ocachecli list | grep -c "evict-key" || true)
    echo "Keys remaining after eviction: $REMAINING_KEYS out of 20"
    if [ "$REMAINING_KEYS" -lt 20 ] && [ "$REMAINING_KEYS" -gt 0 ]; then
        pass_test "RESULT_${policy}_basic" "[${policy}] Eviction occurred: $((20 - REMAINING_KEYS)) keys evicted"
    else
        fail_test "RESULT_${policy}_basic" "[${policy}] Eviction did not work as expected"
    fi

    clear_all_keys

    # ---------------------------------------------------------------------------
    # Test 2: Protection semantics (policy-specific)
    #   LRU  -> reading a key protects it (recently-accessed survive).
    #   FIFO -> reading a key does NOT protect it (oldest-written evict regardless).
    # ---------------------------------------------------------------------------
    echo
    echo "=== [${policy}] Test 2: Protection semantics ==="
    echo "Adding 15 keys in order (order-key-1 oldest .. order-key-15 newest)..."
    for i in $(seq 1 15); do
        VALUE=$(generate_random_data 3500)
        ./ocachecli put "order-key-${i}" "$VALUE" >/dev/null 2>&1 || true
        sleep 0.1
    done

    # Read the OLDEST keys (1-5) under both policies. Under LRU this refreshes
    # them so they survive; under FIFO it must NOT protect them.
    echo "Reading oldest keys (1-5)..."
    for i in $(seq 1 5); do
        ./ocachecli get "order-key-${i}" >/dev/null 2>&1 || true
    done

    echo "Adding 5 more keys (16-20) to push over the limit and trigger eviction..."
    for i in $(seq 16 20); do
        VALUE=$(generate_random_data 3500)
        ./ocachecli put "order-key-${i}" "$VALUE" >/dev/null 2>&1 || true
    done

    echo "Waiting for eviction..."
    sleep 10

    # Newest-written keys (16-20) survive under both policies.
    NEWEST_EXISTS=0
    for i in $(seq 16 20); do
        ./ocachecli get "order-key-${i}" >/dev/null 2>&1 && NEWEST_EXISTS=$((NEWEST_EXISTS + 1)) || true
    done
    # The oldest keys we READ (1-5).
    READ_OLD_EXISTS=0
    for i in $(seq 1 5); do
        ./ocachecli get "order-key-${i}" >/dev/null 2>&1 && READ_OLD_EXISTS=$((READ_OLD_EXISTS + 1)) || true
    done

    echo "Newest keys (16-20) surviving: $NEWEST_EXISTS/5"
    echo "Read-oldest keys (1-5) surviving: $READ_OLD_EXISTS/5"

    if [ "$policy" = "lru" ]; then
        # LRU: the oldest keys we read should be protected (mostly survive).
        if [ "$NEWEST_EXISTS" -ge 3 ] && [ "$READ_OLD_EXISTS" -ge 3 ]; then
            pass_test "RESULT_${policy}_protection" "[lru] Reads protected the accessed keys"
        else
            fail_test "RESULT_${policy}_protection" "[lru] Reads did not protect accessed keys (read-old survived=$READ_OLD_EXISTS)"
        fi
    else
        # FIFO: the oldest keys we read must NOT be protected (mostly evicted),
        # while the newest-written survive.
        if [ "$NEWEST_EXISTS" -ge 3 ] && [ "$READ_OLD_EXISTS" -le 2 ]; then
            pass_test "RESULT_${policy}_protection" "[fifo] Reads did not protect oldest-written keys (evicted despite being read)"
        else
            fail_test "RESULT_${policy}_protection" "[fifo] Oldest-written keys were not evicted (read-old survived=$READ_OLD_EXISTS)"
        fi
    fi

    clear_all_keys

    # ---------------------------------------------------------------------------
    # Test 3: TTL interaction (policy-agnostic)
    #   TTL expiry must work regardless of the eviction policy.
    # ---------------------------------------------------------------------------
    echo
    echo "=== [${policy}] Test 3: TTL interaction ==="
    echo "Adding 5 TTL keys (15s) and 10 regular keys to trigger eviction..."
    for i in $(seq 1 5); do
        VALUE=$(generate_random_data 5000)
        ./ocachecli put "ttl-${i}" "$VALUE" --ttl 15 >/dev/null 2>&1 || true
    done
    for i in $(seq 1 10); do
        VALUE=$(generate_random_data 5000)
        ./ocachecli put "regular-${i}" "$VALUE" >/dev/null 2>&1 || true
    done

    echo "Waiting for eviction, then for TTL expiration..."
    sleep 10
    TTL_BEFORE=$(./ocachecli list | grep -c "ttl-" || true)
    echo "TTL keys before expiration: $TTL_BEFORE"
    sleep 10
    TTL_AFTER=$(./ocachecli list | grep -c "ttl-" || true)
    echo "TTL keys after expiration window: $TTL_AFTER"

    if [ "$TTL_AFTER" -eq 0 ]; then
        pass_test "RESULT_${policy}_ttl" "[${policy}] TTL keys expired correctly alongside eviction"
    else
        fail_test "RESULT_${policy}_ttl" "[${policy}] TTL expiration not working ($TTL_AFTER TTL keys remain)"
    fi

    clear_all_keys

    # ---------------------------------------------------------------------------
    # Test 4: Restart persistence (policy-agnostic)
    # ---------------------------------------------------------------------------
    echo
    echo "=== [${policy}] Test 4: Restart persistence ==="
    echo "Adding keys, then restarting the server with the same disk..."
    for i in $(seq 1 5); do
        VALUE=$(generate_random_data 3000)
        ./ocachecli put "persist-${i}" "$VALUE" >/dev/null 2>&1 || true
    done
    sleep 2

    stop_server
    start_server "${policy}-eviction" "false" \
      -disk "$dir" \
      -threshold 1000 \
      -max-disk-usage 51200 \
      -eviction-policy "$policy" \
      -ttl-cleanup-interval 5s \
      -v

    PERSISTED=$(./ocachecli list | grep -c "persist-" || true)
    echo "Persisted keys after restart: $PERSISTED/5"
    if [ "$PERSISTED" -gt 0 ]; then
        pass_test "RESULT_${policy}_restart" "[${policy}] Data persisted across restart"
    else
        fail_test "RESULT_${policy}_restart" "[${policy}] No data persisted across restart"
    fi

    stop_server
}

# Run the suite for both eviction policies.
run_eviction_suite "lru"
run_eviction_suite "fifo"

# =============================================================================
# Results Summary
# =============================================================================
echo
echo "=== Test Results Summary ==="
echo
for policy in lru fifo; do
    echo "Policy: ${policy}"
    echo "----------------------"
    print_test_result "  Basic eviction" "$(get_result "$policy" basic)"
    print_test_result "  Protection semantics" "$(get_result "$policy" protection)"
    print_test_result "  TTL interaction" "$(get_result "$policy" ttl)"
    print_test_result "  Restart persistence" "$(get_result "$policy" restart)"
    echo
done

print_overall_result

# Exit with appropriate code (cleanup will be called automatically)
if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi
