# Spark Performance Optimization Playbook

> Battle-tested patterns for making Apache Spark jobs **fast and cheap**. Each technique paired with reproducible benchmarks on a local stack — no cloud required. The same patterns scale to EMR, Databricks, Dataproc, and Glue.

[![CI](https://github.com/sushmakl95/spark-performance-optimization-playbook/actions/workflows/ci.yml/badge.svg)](https://github.com/sushmakl95/spark-performance-optimization-playbook/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-E25A1C.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

---

## Author

**Sushma K L** — Senior Data Engineer
📍 Bengaluru, India
💼 [LinkedIn](https://www.linkedin.com/in/sushmakl1995/) • 🐙 [GitHub](https://github.com/sushmakl95) • ✉️ sushmakl95@gmail.com

8+ years of shipping PySpark data pipelines on AWS Glue, Databricks, EMR. This playbook condenses the optimization patterns I reach for most often — the ones that consistently deliver 5-50× speed/cost wins on real production workloads.

---

## Why this repo exists

Most Spark tuning content is either (a) vendor marketing or (b) a dump of configuration flags without the *why*. This repo is different:

- **Every technique has a benchmark**. Not "this should be faster" — actual measured numbers.
- **Every benchmark is reproducible on a laptop**. Docker + synthetic data. No cloud bill.
- **Every optimization explains the internals**. *Why* does broadcast join beat shuffle? *Why* does AQE help with skew? You leave understanding, not memorizing.

If you're a data engineer who wants to stop guessing and start measuring — this is for you.

## What's covered

| # | Technique | Typical impact | When to use |
|---|---|---|---|
| 1 | **File sizing & coalesce strategy** | 3-10× read speed | Small-files problem on S3/HDFS |
| 2 | **Partition pruning** | 5-50× | Time-series + columnar storage |
| 3 | **Z-ORDER / Liquid Clustering** | 10-40× | Multi-dimensional filter predicates |
| 4 | **Broadcast joins** | 5-20× | Small fact × big dim |
| 5 | **Adaptive Query Execution (AQE)** | 2-5× | Skewed joins, dynamic shuffle partitions |
| 6 | **Skew handling (salting)** | 10-100× | Heavily skewed keys |
| 7 | **Executor & memory tuning** | 1.5-3× | All workloads — foundational |
| 8 | **Caching & checkpointing** | 2-10× | Multi-use intermediate DataFrames |
| 9 | **Predicate pushdown** | 3-20× | Filtering on columnar formats |
| 10 | **Storage format choice** | 3-8× | Parquet vs ORC vs Delta vs Iceberg |

Each technique has its own notebook in [`notebooks/`](notebooks/) showing before/after measurements.

## Quick demo

```bash
# Install
git clone https://github.com/sushmakl95/spark-performance-optimization-playbook.git
cd spark-performance-optimization-playbook
make install-dev

# Generate synthetic dataset (1GB, ~10M rows)
make seed-data

# Run the whole benchmark suite
make bench-all
```

Expected output (shortened, on an 8-core laptop):

```
[bench] 01_file_sizing              before=142.3s  after= 14.8s  speedup= 9.6x
[bench] 02_partition_pruning        before=87.2s   after=  3.1s  speedup=28.1x
[bench] 03_zorder_clustering        before=60.1s   after=  5.4s  speedup=11.1x
[bench] 04_broadcast_join           before=48.7s   after=  4.9s  speedup= 9.9x
[bench] 05_aqe_skew                 before=120.3s  after= 39.8s  speedup= 3.0x
[bench] 06_skew_salting             before=240.1s  after=  9.2s  speedup=26.1x
[bench] 07_executor_tuning          before=58.4s   after= 22.1s  speedup= 2.6x
[bench] 08_caching                  before=36.9s   after=  7.8s  speedup= 4.7x
[bench] 09_predicate_pushdown       before=45.7s   after=  6.3s  speedup= 7.3x
[bench] 10_storage_formats          before=82.3s   after=  9.1s  speedup= 9.0x
```

These numbers are from my M-series laptop. Your mileage will vary — but the relative improvements are stable across hardware.

## Repo structure

```
spark-performance-optimization-playbook/
├── notebooks/                  # Jupyter notebooks — one per technique
├── src/playbook/               # Reusable utilities (benchmark harness, etc.)
├── data/generators/            # Synthetic data generators
├── benchmarks/                 # Automated benchmark scripts
├── docs/                       # Deep-dive articles per technique
├── scripts/                    # Local dev helpers
└── infra/                      # Reference IaC snippets (EMR, Glue, Databricks)
```

## Running individual benchmarks

```bash
# Just partition pruning
python -m benchmarks.run_bench --technique partition_pruning

# With custom data scale
python -m benchmarks.run_bench --technique broadcast_join --scale 5x

# Compare before/after side by side (includes Spark UI metrics)
python -m benchmarks.compare --technique zorder_clustering --output report.md
```

## Local Spark setup

Default uses **PySpark local mode** (embedded Spark, no cluster). Everything runs on one machine — good enough to observe the ratios, not absolute numbers.

For realistic distributed behavior, spin up the included Docker Compose (1 driver + 3 workers):

```bash
docker compose -f compose/docker-compose.yml up -d
# SparkUI at http://localhost:4040 during jobs
```

## Who should read this

- **Data engineers** tuning Glue/EMR/Databricks jobs and unsure which lever matters most
- **Analytics engineers** hitting perf walls with dbt + Spark
- **Platform teams** writing internal tuning guides
- **Interview candidates** preparing for Spark-heavy system design rounds

## What this repo is NOT

- ❌ A Spark introduction — assumes you know `DataFrame`, `join`, `window`, partitioning basics
- ❌ A vendor-specific tuning guide — patterns apply across platforms
- ❌ A list of configs without explanation — every setting is paired with the internals

## Reference reading

- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) (canonical)
- [Databricks: Adaptive Query Execution](https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
- Holden Karau & Rachel Warren, *High Performance Spark* (O'Reilly)
- Jacek Laskowski, *Mastering Spark SQL* (free online book)

## License

MIT — see [LICENSE](LICENSE).
