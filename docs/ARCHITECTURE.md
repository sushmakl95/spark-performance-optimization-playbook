# Architecture

## What this project is

A **benchmark harness** + **10 reproducible experiments** demonstrating Spark performance optimization techniques. Not a library to import into your pipelines — a reference you clone, run locally, and apply the lessons.

```
spark-performance-optimization-playbook/
├── src/playbook/                    # Reusable utilities
│   ├── spark_session.py             # Baseline vs Optimized SparkSession factories
│   └── benchmark.py                 # Measurement harness (median, p95, warmup)
├── data/generators/
│   └── ecommerce.py                 # Configurable-scale synthetic data generator
├── benchmarks/                      # One script per technique
│   ├── bench_file_sizing.py
│   ├── bench_partition_pruning.py
│   ├── bench_broadcast_join.py
│   ├── bench_skew_handling.py
│   ├── bench_aqe.py
│   ├── bench_caching.py
│   ├── bench_predicate_pushdown.py
│   ├── bench_storage_formats.py
│   ├── bench_zorder.py
│   ├── bench_executor_tuning.py
│   └── run_bench.py                 # CLI entrypoint
├── tests/                           # Unit tests for the harness
├── docs/                            # Deep-dive explanations
└── infra/                           # Reference IaC for EMR/Databricks/Glue
```

## The benchmark harness

Every benchmark follows the same pattern:

```python
from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark

spark = get_optimized_spark("bench-my-technique")
bench = Benchmark(spark, "my_technique", iterations=3, warmup=1)

before = bench.run("baseline", lambda: slow_query())
after  = bench.run("optimized", lambda: fast_query())

print(compare(before, after))
```

The harness handles:
- **Warmup runs** (excluded from measurements — JVM/codegen cache cold-starts inflate first runs by 2-3×)
- **Multiple iterations with median** (single-run numbers have 10-30% variance)
- **Cache clearing between runs** (avoids cross-contamination)
- **Result extraction** (row counts, eventually Spark UI metrics)

## The two SparkSession factories

Every benchmark needs to compare "before" vs "after". The `spark_session` module exposes two factories that make the comparison explicit:

**`get_spark()`** — baseline. Defaults only. AQE explicitly disabled so it can be measured independently.

**`get_optimized_spark()`** — all recommended tuning applied:
- AQE + skew join handling
- Shuffle partitions = 2 × cores (vs 200 default that fragments small jobs)
- Kryo serializer
- Broadcast threshold = 100MB (vs 10MB default)
- CBO + histogram stats

Pick whichever matches what you're measuring. Override with `extra_configs` for technique-specific tuning.

## The data generator

`data/generators/ecommerce.py` produces a realistic 4-table e-commerce dataset (customers, products, orders, order_items) at configurable scale:

| Scale | Rows | Compressed size | Use case |
|---|---|---|---|
| 0.1x | 10k orders | ~5 MB | Quick CI / unit tests |
| 1x | 100k orders | ~50 MB | Laptop benchmarks |
| 10x | 1M orders | ~500 MB | Realistic perf characterization |
| 100x | 10M orders | ~5 GB | Prod-ish stress test |

Critical features for benchmarks:
- **Date-partitioned orders**: `partitionBy("order_date")` for pruning benchmarks
- **Optional skew mode** (`--skewed`): 30% of orders to 0.1% of customers → classic power-law skew
- **Realistic cardinality**: 5k products, 100k customers — like real e-commerce ratios

## Why local Spark for benchmarks?

Two reasons:

1. **Reproducibility**. Cloud benchmarks have too many variables (network, shared tenancy, throttling). Local runs are noisy but the ratios are stable.
2. **Cost**. The techniques that matter (partition pruning, broadcast joins, AQE) show their shape at 100MB-10GB just as clearly as at 1TB. You don't need a $500 EMR cluster to learn them.

For the *absolute* numbers, scale up in a real cluster. The techniques are universal.

## Why separate variants instead of a unified framework?

Each benchmark is **a standalone script** that tells one story. Runnable independently. Self-contained. No plugin registry, no config files, no abstraction tax.

This makes the repo a **reading project** as much as a running project — you can understand every file in 5 minutes, and copy-paste any technique into your own codebase with no dependencies on this repo.

## When to use which technique

Use the decision tree in [docs/DECISION_TREE.md](DECISION_TREE.md).

Short version:
- **Slow reads on S3?** → file_sizing (consolidate to 128-256MB files)
- **Time-series queries?** → partition_pruning (partitionBy date column)
- **Multi-column filters?** → zorder (Delta Z-ORDER on 2-4 columns)
- **Fact × small dim join?** → broadcast_join (up auto-broadcast threshold)
- **Shuffle-heavy job with stragglers?** → aqe (turn it on; free win) or skew_handling if AQE isn't enough
- **Same DF used 2+ times?** → caching
- **Columns wrapped in functions?** → predicate_pushdown (normalize at ingest)
- **Reading ORC for no reason?** → storage_formats (use Parquet or Delta)
- **Too many tiny tasks?** → executor_tuning (bigger executors)

## Trade-offs baked into every technique

No free lunches:

| Optimization | Cost |
|---|---|
| partitionBy | Slower writes, more files |
| Z-ORDER | ~2× longer write; needs periodic re-OPTIMIZE |
| broadcast join | Memory on each executor |
| caching | Memory; materialization cost |
| AQE | Minor overhead when not useful; ~zero actual downside |
| big executors | GC pauses beyond ~32GB heap |

The benchmarks explicitly call these out in their logs.

## Reference reading

- [Spark SQL Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Holden Karau — High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/)
- [Databricks AQE blog post](https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
- [Jacek Laskowski — Mastering Spark SQL](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/)
