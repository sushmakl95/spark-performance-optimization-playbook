"""Benchmark: Partition pruning on date-partitioned Parquet.

The question this answers: how much does partition pruning actually save on a
time-window query?

Setup: orders table, partitioned by order_date, ~1M rows spread across 1000 days.
Query: "give me orders in the last 30 days".

Expected results:
  - full-scan: reads all 1000 days of parquet files
  - pruned: reads only 30 days → ~33× less I/O

Why this matters: most analytical queries have a date predicate. Getting
partition pruning right is the single biggest win for time-series workloads.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-partition-pruning")

    # Partitioned orders (written via partitionBy("order_date"))
    partitioned_path = str(data_dir / "orders")

    # For "baseline" comparison, copy orders to a non-partitioned location
    # This mirrors the real-world decision: partition at write time or not?
    unpartitioned_path = str(data_dir / "_bench" / "orders_unpartitioned")
    if not Path(unpartitioned_path).exists():
        (
            spark.read.parquet(partitioned_path)
            .coalesce(8)
            .write.mode("overwrite")
            .parquet(unpartitioned_path)
        )

    # 30-day cutoff predicate
    cutoff = spark.sql("SELECT date_sub(current_date(), 30)").first()[0]

    def query_unpartitioned():
        return (
            spark.read.parquet(unpartitioned_path)
            .filter(F.col("order_date") >= F.lit(cutoff))
            .groupBy("order_status")
            .agg(F.sum("total_amount").alias("revenue"), F.count("*").alias("n"))
            .collect()
        )

    def query_partitioned():
        return (
            spark.read.parquet(partitioned_path)
            .filter(F.col("order_date") >= F.lit(cutoff))
            .groupBy("order_status")
            .agg(F.sum("total_amount").alias("revenue"), F.count("*").alias("n"))
            .collect()
        )

    bench = Benchmark(spark, "partition_pruning", iterations=3, warmup=1)
    before = bench.run("unpartitioned", lambda: len(query_unpartitioned()))
    after = bench.run("partitioned", lambda: len(query_partitioned()))

    print(compare(before, after))

    # Print the physical plans to show what Spark actually did
    print("\n--- UNPARTITIONED PHYSICAL PLAN ---")
    spark.read.parquet(unpartitioned_path).filter(F.col("order_date") >= F.lit(cutoff)).explain()
    print("\n--- PARTITIONED PHYSICAL PLAN (look for PartitionFilters) ---")
    spark.read.parquet(partitioned_path).filter(F.col("order_date") >= F.lit(cutoff)).explain()

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
