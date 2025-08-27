# Run Benchmarks

Run performance benchmarks against the cache service.

```bash
./ocachecli bench [options]
```

**Options:**

- `--concurrency value`: Number of concurrent workers (default: 8)
- `--num-keys value`: Number of unique keys (default: 1000)
- `--num-ops value`: Total number of operations (default: 10000)
- `--value-size value`: Value size in bytes (default: 100)
- `--workload value`: Workload type or custom mix (default: "A")
- `--force-streaming`: Force streaming for all operations regardless of size
- `--no-progress`: Disable progress output during benchmark

**Workload Types:**

| Type   | Description  | Read %                         | Update % |
| ------ | ------------ | ------------------------------ | -------- |
| A      | Update heavy | 50                             | 50       |
| B      | Read mostly  | 95                             | 5        |
| C      | Read only    | 100                            | 0        |
| Custom | User defined | Specify as `read=70,update=30` |          |

**Examples:**

First, run the server with default settings:

```bash
make run
```

Then, run the benchmark:

```bash
# Run default benchmark (Workload A)
./ocachecli bench

# Run read-heavy benchmark with more operations
./ocachecli bench --workload B --num-ops 100000

# Run with custom workload mix
./ocachecli bench --workload "read=70,update=30"

# High concurrency test
./ocachecli bench --concurrency 50 --num-keys 10000

# Large value test
./ocachecli bench --value-size 1000000 --num-ops 1000

# Comprehensive benchmark
./ocachecli bench \
  --workload B \
  --concurrency 16 \
  --num-keys 5000 \
  --num-ops 50000 \
  --value-size 1000
```

**Benchmark Examples by Object Size:**

```bash
# Run all three sizes sequentially to test cache behavior across object sizes
echo "=== Testing 100 byte objects ==="
./ocachecli bench --value-size 100 --num-ops 10000 --workload A

echo "=== Testing 100 KB objects ==="
./ocachecli bench --value-size 100000 --num-ops 10000 --workload B

echo "=== Testing 1 MB objects ==="
./ocachecli bench --value-size 1000000 --num-ops 10000 --workload C
```

**Output:**
The benchmark command provides detailed statistics including:

- Total operations completed
- Operations per second (throughput)
- Latency percentiles (p50, p95, p99)
- Operation breakdown by type
- Error count (if any)
