#!/usr/bin/env bash
# Sample process memory from /proc/<pid>/status at a fixed interval until the
# sampler is killed, recording the aggregate across all given PIDs. Used by the
# benchmark workflows to compare allocator RSS (plain vs jemalloc) — the metric
# issue #176 exists to move. Linux only (/proc); the benchmark runners are Linux.
#
# Records the sum across PIDs of:
#   VmRSS   - resident set size (anon + file-backed pages)
#   RssAnon - anonymous RSS; this is the allocator/arena footprint #176 targets
#   VmData  - virtual data segment; reserved-but-unfreed arena space (glibc
#             fragmentation shows up as VmData sitting well above RssAnon)
#
# Usage: rss-sample.sh <out.csv> <interval_seconds> <pid> [pid ...]
set -uo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <out.csv> <interval_seconds> <pid> [pid ...]" >&2
  exit 2
fi

OUT="$1"; INTERVAL="$2"; shift 2
PIDS=("$@")

echo "elapsed_s,VmRSS_kb,RssAnon_kb,VmData_kb" > "$OUT"
start=$(date +%s)

# field <Key> <status-file> -> the kB value for that key (empty if absent)
field() { awk -v k="$1:" '$1==k {print $2; exit}' "$2" 2>/dev/null; }

while :; do
  alive=0; rss=0; anon=0; data=0
  for pid in "${PIDS[@]}"; do
    st="/proc/${pid}/status"
    [ -r "$st" ] || continue
    alive=1
    v=$(field VmRSS "$st");   rss=$((rss + ${v:-0}))
    a=$(field RssAnon "$st"); anon=$((anon + ${a:-0}))
    d=$(field VmData "$st");  data=$((data + ${d:-0}))
  done
  [ "$alive" -eq 0 ] && break
  now=$(date +%s)
  echo "$((now - start)),${rss},${anon},${data}" >> "$OUT"
  sleep "$INTERVAL"
done
