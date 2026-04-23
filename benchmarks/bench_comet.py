"""Benchmark: Apache DataFusion Comet vs vanilla Spark execution.

**Apache DataFusion Comet** (https://datafusion.apache.org/comet/) is a
Rust-based native columnar accelerator for Spark. It hooks into Spark's
physical plan and replaces row-by-row Java UDF execution with vectorized
Arrow-backed operators.

On analytic queries with heavy aggregations / predicates, Comet commonly
delivers 2-4x end-to-end speedups without changing application code - you
install the jar, set a single `spark.plugins` config, and the optimizer
routes compatible operators to the native runtime.

When to reach for Comet:
- Heavy parquet scans with selective filters
- Aggregations over numeric columns (SUM, AVG, HyperLogLog approx)
- Complex expressions with many CASE / math ops

When Comet doesn't help:
- UDF-heavy code (stays on JVM)
- Streaming joins with stateful operators
- Workloads dominated by shuffle (Comet accelerates scans + expressions,
  not the shuffle fabric itself)

Comet is in-preview at the time of writing; production use requires pinning
the version that matches your Spark version exactly.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare


def _spark(app_name: str, comet: bool) -> SparkSession:  # pragma: no cover - runtime
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
    )
    if comet:
        builder = (
            builder
            # jars installed via --packages in your spark-submit; config shown
            # here is illustrative and must match your Comet release.
            .config("spark.plugins", "org.apache.spark.CometPlugin")
            .config("spark.comet.enabled", "true")
            .config("spark.comet.exec.enabled", "true")
            .config("spark.comet.explainFallback.enabled", "true")
        )
    return builder.getOrCreate()


def benchmark(data_dir: Path) -> None:  # pragma: no cover - runtime benchmark
    orders_path = str(data_dir / "orders")

    def analytic_query(spark: SparkSession) -> int:
        return (
            spark.read.parquet(orders_path)
            .filter(F.col("order_date") >= "2024-01-01")
            .groupBy("customer_country", F.col("order_status"))
            .agg(
                F.sum("total_amount").alias("gmv"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.avg("total_amount").alias("avg_order"),
            )
            .count()
        )

    vanilla = _spark("bench-comet-vanilla", comet=False)
    v_bench = Benchmark(vanilla, "vanilla_spark", iterations=3, warmup=1)
    v = v_bench.run("vanilla", lambda: analytic_query(vanilla))
    vanilla.stop()

    native = _spark("bench-comet-native", comet=True)
    n_bench = Benchmark(native, "datafusion_comet", iterations=3, warmup=1)
    n = n_bench.run("comet", lambda: analytic_query(native))
    native.stop()

    print(compare(v, n))
    print("\nTakeaways:")
    print("  - Comet accelerates scans + expressions; shuffle stays JVM")
    print("  - No code changes; install jar + enable plugin")
    print("  - Pin the Comet version exactly to your Spark version")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
