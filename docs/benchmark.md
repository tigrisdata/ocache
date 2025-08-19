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
# Small objects (100 bytes)
./ocachecli bench \
  --value-size 100 \
  --num-keys 10000 \
  --num-ops 100000 \
  --workload A

# Medium objects (100 KB)
./ocachecli bench \
  --value-size 100000 \
  --num-keys 1000 \
  --num-ops 10000 \
  --workload B

# Large objects (1 MB)
./ocachecli bench \
  --value-size 1000000 \
  --num-keys 1000 \
  --num-ops 10000 \
  --workload C

# Mixed workload benchmark suite
# Run all three sizes sequentially to test cache behavior across object sizes
echo "=== Testing 100 byte objects ==="
./ocachecli bench --value-size 100 --num-ops 10000 --workload A

echo "=== Testing 100 KB objects ==="
./ocachecli bench --value-size 100000 --num-ops 10000 --workload B

echo "=== Testing 1 MB objects ==="
./ocachecli bench --value-size 1000000 --num-ops 10000 --workload C
```

**Performance Tuning Examples:**

```bash
# Small objects with high concurrency
./ocachecli bench \
  --value-size 100 \
  --num-keys 100000 \
  --num-ops 1000000 \
  --workload B \
  --concurrency 32

# Large objects with sequential access
./ocachecli bench \
  --value-size 1000000 \
  --num-keys 1000 \
  --num-ops 10000 \
  --workload B \
  --concurrency 8

# Medium objects with mixed read/write
./ocachecli bench \
  --value-size 100000 \
  --num-keys 1000 \
  --num-ops 10000 \
  --workload A \
  --concurrency 8
```

**Output:**
The benchmark command provides detailed statistics including:

- Total operations completed
- Operations per second (throughput)
- Latency percentiles (p50, p95, p99)
- Operation breakdown by type
- Error count (if any)
