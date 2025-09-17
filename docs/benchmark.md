# Run Benchmarks

Run performance benchmarks against the cache service.

```bash
ocachecli bench [options]
```

**Options:**

| Flag                | Description                           | Default           |
| ------------------- | ------------------------------------- | ----------------- |
| `--num-keys`        | Number of unique keys                 | `1000`            |
| `--value-size`      | Size of each value in bytes           | `100`             |
| `--num-ops`         | Total number of operations            | `10000`           |
| `--concurrency`     | Number of concurrent workers          | `8`               |
| `--workload`        | Workload type (A, B, C) or custom mix | `A`               |
| `--seed`            | Random seed for reproducibility       | Current timestamp |
| `--no-progress`     | Disable progress output               | `false`           |
| `--force-streaming` | Force streaming for all operations    | `false`           |

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
# Basic benchmark with default settings
ocachecli bench

# Custom workload with more operations
ocachecli bench \
  --num-keys 10000 \
  --num-ops 100000 \
  --concurrency 16 \
  --value-size 1024 \
  --workload B

# Benchmark with multiple servers and custom pool size
ocachecli \
  --addr "node1:9001,node2:9002,node3:9003" \
  --pool-size 10 \
  bench \
  --num-keys 50000 \
  --num-ops 500000 \
  --concurrency 32
```

**Benchmark Examples by Object Size:**

```bash
# Run all three sizes sequentially to test cache behavior across object sizes
echo "=== Testing 100 byte objects ==="
ocachecli bench --value-size 100 --num-ops 10000 --workload A

echo "=== Testing 100 KB objects ==="
ocachecli bench --value-size 100000 --num-ops 10000 --workload B

echo "=== Testing 1 MB objects ==="
ocachecli bench --value-size 1000000 --num-ops 10000 --workload C
```

**Output:**
The benchmark command provides detailed statistics including:

- Total operations completed
- Operations per second (throughput)
- Latency percentiles (p50, p95, p99)
- Operation breakdown by type
- Error count (if any)
