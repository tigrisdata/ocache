#!/usr/bin/env bash
# Copyright 2026 Tigris Data, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Run one CacheService.GetTopology benchmark shape from the prebuilt test binary.
# The CPU mode emits latency, allocation-count, and request-time token-sort
# profile metrics. The latency mode emits latency and allocation-count metrics
# for the same gRPC route.

set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage: measure-topology-benchmark.sh <cpu|latency> <active-nodes> <tokens-per-node> <benchtime>
EOF
	exit 2
}

if [[ $# -ne 4 ]]; then
	usage
fi

mode=$1
active_nodes=$2
tokens_per_node=$3
benchtime=$4

case "$mode" in
cpu | latency) ;;
*) usage ;;
esac

if ! [[ $active_nodes =~ ^[1-9][0-9]*$ && $tokens_per_node =~ ^[1-9][0-9]*$ ]]; then
	echo "active-nodes and tokens-per-node must be positive integers" >&2
	exit 2
fi

: "${PERFLOOP_BUILD_OUTPUT_DIR:?PERFLOOP_BUILD_OUTPUT_DIR is required}"
test_binary="$PERFLOOP_BUILD_OUTPUT_DIR/cache-service-topology.test"
profile_binary="$PERFLOOP_BUILD_OUTPUT_DIR/topology-profile"
if [[ ! -x $test_binary || ! -x $profile_binary ]]; then
	echo "topology benchmark artifacts are missing from $PERFLOOP_BUILD_OUTPUT_DIR" >&2
	exit 1
fi

measure_dir=${PERFLOOP_MEASURE_DIR:-${TMPDIR:-/tmp}}
mkdir -p "$measure_dir"
report=$(mktemp "$measure_dir/topology-benchmark.XXXXXX")
profile=
cleanup() {
	rm -f "$report"
	if [[ -n $profile ]]; then
		rm -f "$profile"
	fi
}
trap cleanup EXIT

benchmark="BenchmarkCacheServiceGetTopology/nodes=${active_nodes}/tokens=${tokens_per_node}"
run_benchmark() {
	"$test_binary" \
		-test.run='^$' \
		-test.bench="^${benchmark}$" \
		-test.count=1 \
		-test.benchmem \
		-test.benchtime="$benchtime" \
		"$@" \
		>"$report" 2>&1
}

emit_benchmark_metrics() {
	# The adapter emits B/op for every Go benchmark line. This workload reports
	# allocation counts rather than byte estimates.
	perfloop-go-bench-json "$benchmark" 'ns/op' 'allocs/op' <"$report" |
		sed '/"metric":"B\/op"/d'
}

if [[ $mode == cpu ]]; then
	profile=$(mktemp "$measure_dir/topology-cpu.XXXXXX")
	rm -f "$profile"
	run_benchmark -test.cpuprofile="$profile"
	emit_benchmark_metrics
	sort_cpu_ppm=$("$profile_binary" -profile "$profile")
	if ! [[ $sort_cpu_ppm =~ ^[0-9]+$ ]]; then
		echo "topology profile reported an invalid sort_cpu_ppm value: $sort_cpu_ppm" >&2
		exit 1
	fi
	printf '{"metric":"sort_cpu_ppm","value":%s}\n' "$sort_cpu_ppm"
else
	run_benchmark
	emit_benchmark_metrics
fi
