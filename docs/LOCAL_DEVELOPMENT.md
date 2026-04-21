# Local Development

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Runtime |
| Java | 17 | Required by Spark 3.5 |
| Docker (optional) | 24+ | For realistic Spark cluster stack |
| Make | any | Convenience |

## Setup

```bash
git clone https://github.com/sushmakl95/spark-performance-optimization-playbook.git
cd spark-performance-optimization-playbook
make install-dev
source .venv/bin/activate
```

## Quick tour

```bash
# 1. Generate a synthetic dataset (1x scale, ~50MB)
make seed-data

# 2. Run one benchmark
python -m benchmarks.run_bench --technique partition_pruning

# 3. Run all 10 benchmarks
make bench-all

# 4. Explore interactively
jupyter lab
```

## Two Spark environments

### Local mode (default, fastest)

`PySpark` in a single JVM. All benchmarks in the repo run here. Good for:
- Iterating on benchmark scripts
- Understanding the techniques
- CI

**Limitation**: no multi-executor parallelism, no real network shuffle. Numbers reflect ratios, not absolutes.

### Docker Compose cluster (realistic)

`compose/docker-compose.yml` spins up:
- 1 Spark master
- 3 Spark workers (2 cores each)
- Shared host mount for data

```bash
docker compose -f compose/docker-compose.yml up -d
# Spark UI at http://localhost:8080
# Jobs from your laptop: --master spark://localhost:7077

docker compose -f compose/docker-compose.yml down
```

Use this when you want to see *real* shuffle, skew, and executor tuning dynamics — otherwise stick to local.

## Running benchmarks

All benchmarks share the same CLI shape:

```bash
python -m benchmarks.bench_<technique> --data-dir data/generated
```

Or via the unified runner:

```bash
python -m benchmarks.run_bench --technique <name>
python -m benchmarks.run_bench --technique all
```

Results print to stdout in the format:

```
[bench] partition_pruning              before= 87.20s  after=  3.10s  speedup=28.1x  rows=✓32,741
```

## Inspecting execution

Every benchmark prints the physical plan for its variants. Look for these key operators:

| Operator | What it means |
|---|---|
| `FileScan parquet [...] PushedFilters: [...]` | Predicate pushed into reader ✓ |
| `FileScan parquet [...] PartitionFilters: [...]` | Partition pruned ✓ |
| `BroadcastHashJoin` | Small side broadcast ✓ |
| `SortMergeJoin` | Both sides shuffled |
| `ShuffledHashJoin` | Both sides shuffled (variant) |
| `AdaptiveSparkPlan isFinalPlan=true` | AQE finalized its re-optimization |
| `Exchange hashpartitioning(...)` | Shuffle — expensive |

## Generating larger datasets

The default 1x dataset is 50MB. For more realistic numbers:

```bash
python -m data.generators.ecommerce --scale 10x   # ~500MB, ~5 min
python -m data.generators.ecommerce --scale 100x  # ~5GB, ~30 min
```

For skew-handling benchmarks, use `--skewed`:

```bash
python -m data.generators.ecommerce --scale 10x --skewed
```

This attributes 30% of orders to the top 0.1% of customers — the classic power-law distribution that makes joins painful.

## Testing

```bash
# Fast unit tests (benchmark harness logic)
make test-unit

# Lint + type-check + security
make lint
make typecheck
make security

# Full local CI simulation
make all
```

## Common issues

### `java.lang.NoClassDefFoundError` on Spark startup
Ensure JAVA_HOME points to Java 17, not 21 (Spark 3.5 incompatible with 21).

### `py4j.protocol.Py4JJavaError: An error occurred while calling ... delta`
Delta Lake jars aren't downloaded. They're pulled by `delta-spark` — make sure you installed with `pip install -e ".[dev]"`.

### Benchmark shows no speedup / slight slowdown
- First time running? Make sure warmup=1 (default).
- Dataset too small? Scale up: `--scale 10x`.
- Local mode masks the optimization? Spin up the Docker cluster.

### Out of memory on 10x+ scale
Edit `get_spark()` / `get_optimized_spark()` in `src/playbook/spark_session.py` and bump `spark.driver.memory`.

## Editor setup

Same as our other repos. VS Code + Python + ruff + mypy extensions. PyCharm works. Mark `src/` as sources root.

## Contributing workflow

```bash
git checkout -b feat/my-benchmark
# Add benchmarks/bench_<new>.py
# Add an entry in benchmarks/run_bench.py's BENCH_REGISTRY
# Write a docstring explaining the mechanic + expected result
make ci
git commit -m "feat: add <technique> benchmark"
git push origin feat/my-benchmark
# open PR
```

CI runs ruff + mypy + bandit + terraform fmt. Unit tests run locally.
