#!/usr/bin/env bash

# Source common functions
source "$(dirname "$0")/common.sh"

echo "=== OCache Conditional (CAS) Operations E2E Test ==="
echo
echo "This suite covers the CAS-specific semantics that have no natural home in"
echo "the other e2e suites. CAS interaction with background processes (compaction,"
echo "TTL cleaner) and mixed concurrency is exercised inside compaction_test.sh,"
echo "ttl_cleaner_test.sh, storage_layers_test.sh and concurrent_ops_test.sh."
echo

init_common

TEST_BASIC_CAS=""
TEST_MISMATCH_SEMANTICS=""
TEST_PUT_IF_ABSENT=""
TEST_CONCURRENT_SINGLE_WINNER=""
TEST_STREAMING_CAS=""

# CAS-put mismatch exit code (must match casMismatchExitCode in client/cmd/cas.go).
CAS_MISMATCH=3

start_server "cas" "true" \
  -disk /tmp/ocache-cas-test \
  -threshold 1024 \
  -v

# cas_found <key> -> echoes true/false.
cas_found() {
    ./ocachecli get-with-version "$1" 2>/dev/null | grep -oE 'found=(true|false)' | cut -d= -f2
}

# ============================================================================
echo "=== Test 1: Basic CAS lifecycle (create, guarded update, delete) ==="
out=$(./ocachecli put-if-version "cas-basic" "v1" --expected 0 2>/dev/null); rc=$?
v1=$(echo "$out" | grep -oE 'new_version=[0-9]+' | cut -d= -f2)
if [ "$rc" -eq 0 ] && [ -n "$v1" ] && [ "$v1" -gt 0 ] 2>/dev/null; then
    out=$(./ocachecli put-if-version "cas-basic" "v2" --expected "$v1" 2>/dev/null); rc=$?
    v2=$(echo "$out" | grep -oE 'new_version=[0-9]+' | cut -d= -f2)
    got=$(./ocachecli get "cas-basic" 2>/dev/null)
    if [ "$rc" -eq 0 ] && [ "$v2" -gt "$v1" ] 2>/dev/null && [ "$got" = "v2" ]; then
        ./ocachecli delete-if-version "cas-basic" --expected "$v2" >/dev/null 2>&1; rc=$?
        if [ "$rc" -eq 0 ] && [ "$(cas_found cas-basic)" = "false" ]; then
            pass_test "TEST_BASIC_CAS" "create -> guarded update -> guarded delete works end to end"
        else
            fail_test "TEST_BASIC_CAS" "guarded delete did not remove the key"
        fi
    else
        fail_test "TEST_BASIC_CAS" "guarded update failed (rc=$rc v2=$v2 got=$got)"
    fi
else
    fail_test "TEST_BASIC_CAS" "put-if-absent create failed (rc=$rc v1=$v1)"
fi

# ============================================================================
echo "=== Test 2: Mismatch semantics (stale token is rejected) ==="
out=$(./ocachecli put-if-version "cas-mm" "a" --expected 0 2>/dev/null)
v1=$(echo "$out" | grep -oE 'new_version=[0-9]+' | cut -d= -f2)
out=$(./ocachecli put-if-version "cas-mm" "b" --expected 999999 2>/dev/null); rc=$?
cur=$(echo "$out" | grep -oE 'current_version=[0-9]+' | cut -d= -f2)
val=$(./ocachecli get "cas-mm" 2>/dev/null)
if [ "$rc" -eq "$CAS_MISMATCH" ] && [ "$cur" = "$v1" ] && [ "$val" = "a" ]; then
    pass_test "TEST_MISMATCH_SEMANTICS" "stale token rejected (exit $CAS_MISMATCH, current=$cur), value unchanged"
else
    fail_test "TEST_MISMATCH_SEMANTICS" "expected mismatch exit $CAS_MISMATCH/current=$v1/value=a, got rc=$rc cur=$cur val=$val"
fi

# ============================================================================
echo "=== Test 3: put-if-absent rejects an existing key ==="
./ocachecli put-if-version "cas-pia" "first" --expected 0 >/dev/null 2>&1
out=$(./ocachecli put-if-version "cas-pia" "second" --expected 0 2>/dev/null); rc=$?
val=$(./ocachecli get "cas-pia" 2>/dev/null)
if [ "$rc" -eq "$CAS_MISMATCH" ] && [ "$val" = "first" ]; then
    pass_test "TEST_PUT_IF_ABSENT" "put-if-absent on an existing key is rejected, value preserved"
else
    fail_test "TEST_PUT_IF_ABSENT" "expected rc=$CAS_MISMATCH/value=first, got rc=$rc val=$val"
fi

# ============================================================================
echo "=== Test 4: Streaming CAS (value beyond the unary path) ==="
# Runs before the concurrency test so the large-object streaming path is
# exercised before many short-lived CLI connections accumulate (which can
# exhaust ephemeral ports on some dev machines; CI on Linux is unaffected).
# Stream a 2MB value in via stdin (put-if-absent), read it back via the
# streaming get, and do a guarded streaming update.
STREAM_ERRORS=0
head -c 2097152 /dev/urandom > /tmp/ocache-cas-stream.bin
sum_in=$(md5 -q /tmp/ocache-cas-stream.bin 2>/dev/null || md5sum /tmp/ocache-cas-stream.bin | awk '{print $1}')
out=$(./ocachecli put-if-version "cas-stream" --expected 0 < /tmp/ocache-cas-stream.bin 2>/dev/null); rc=$?
sv=$(echo "$out" | grep -oE 'new_version=[0-9]+' | cut -d= -f2)
if [ "$rc" -ne 0 ] || [ -z "$sv" ]; then
    echo -e "${RED}✗ streaming put-if-version failed (rc=$rc out=$out)${NC}"
    ((STREAM_ERRORS++))
fi
# Metadata reports the streamed length without buffering.
glen=$(./ocachecli get-with-version "cas-stream" 2>/dev/null | grep -oE 'length=[0-9]+' | cut -d= -f2)
if [ "$glen" != "2097152" ]; then
    echo -e "${RED}✗ streaming get-with-version length=$glen (expected 2097152)${NC}"
    ((STREAM_ERRORS++))
fi
# --value streams the payload back byte-for-byte.
./ocachecli get-with-version "cas-stream" --value 2>/dev/null > /tmp/ocache-cas-stream.out
sum_out=$(md5 -q /tmp/ocache-cas-stream.out 2>/dev/null || md5sum /tmp/ocache-cas-stream.out | awk '{print $1}')
if [ "$sum_out" != "$sum_in" ]; then
    echo -e "${RED}✗ streamed value round-trip mismatch${NC}"
    ((STREAM_ERRORS++))
fi
# A guarded streaming update with the right version applies; a stale one fails.
head -c 2097152 /dev/urandom | ./ocachecli put-if-version "cas-stream" --expected "$sv" >/dev/null 2>&1; urc=$?
head -c 4096 /dev/urandom | ./ocachecli put-if-version "cas-stream" --expected "$sv" >/dev/null 2>&1; src=$?
if [ "$urc" -ne 0 ] || [ "$src" -ne "$CAS_MISMATCH" ]; then
    echo -e "${RED}✗ guarded streaming update: update rc=$urc (want 0), stale rc=$src (want $CAS_MISMATCH)${NC}"
    ((STREAM_ERRORS++))
fi
rm -f /tmp/ocache-cas-stream.bin /tmp/ocache-cas-stream.out
if [ "$STREAM_ERRORS" -eq 0 ]; then
    pass_test "TEST_STREAMING_CAS" "2MB streaming put/get/guarded-update round-trips over the wire"
else
    fail_test "TEST_STREAMING_CAS" "$STREAM_ERRORS streaming CAS checks failed"
fi

# ============================================================================
echo "=== Test 5: Concurrency — exactly one winner under contention ==="
./ocachecli put-if-version "cas-race" "base" --expected 0 >/dev/null 2>&1
base_ver=$(./ocachecli get-with-version "cas-race" 2>/dev/null | grep -oE 'version=[0-9]+' | cut -d= -f2)
contenders=6
race_dir=$(mktemp -d)
race_pids=()
for i in $(seq 1 $contenders); do
    (
        ./ocachecli put-if-version "cas-race" "winner-$i" --expected "$base_ver" >/dev/null 2>&1
        echo "$?" > "$race_dir/rc-$i"
    ) &
    race_pids+=($!)
done
# Wait only for the contenders by PID: a bare `wait` would also block on the
# backgrounded server that start_server launched, which never exits (the other
# concurrent e2e suites track PIDs for the same reason).
wait "${race_pids[@]}"
wins=0
mismatches=0
for i in $(seq 1 $contenders); do
    rc=$(cat "$race_dir/rc-$i" 2>/dev/null)
    if [ "$rc" = "0" ]; then wins=$((wins+1)); fi
    if [ "$rc" = "$CAS_MISMATCH" ]; then mismatches=$((mismatches+1)); fi
done
rm -rf "$race_dir"
if [ "$wins" -eq 1 ] && [ "$mismatches" -eq $((contenders-1)) ]; then
    pass_test "TEST_CONCURRENT_SINGLE_WINNER" "exactly one of $contenders contenders won ($mismatches mismatched)"
else
    fail_test "TEST_CONCURRENT_SINGLE_WINNER" "expected 1 win / $((contenders-1)) mismatch, got wins=$wins mismatches=$mismatches"
fi

echo
echo "=== Test Results Summary ==="
echo
print_test_result "Basic CAS lifecycle" "$TEST_BASIC_CAS"
print_test_result "Mismatch semantics" "$TEST_MISMATCH_SEMANTICS"
print_test_result "Put-if-absent" "$TEST_PUT_IF_ABSENT"
print_test_result "Concurrency single winner" "$TEST_CONCURRENT_SINGLE_WINNER"
print_test_result "Streaming CAS" "$TEST_STREAMING_CAS"

print_overall_result

if [ "$TEST_PASSED" = true ]; then
    exit 0
else
    exit 1
fi
