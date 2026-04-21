"""Benchmark: Caching vs recomputation for multi-use DataFrames.

The question: when the same DataFrame is used 2+ times downstream, how much
does `.cache()` save?

The trap: caching has a cost (memory + initial materialization). If the DF
is only used once, caching is strictly worse.

Rules of thumb:
  - Used 2+ times → cache
  - Hot path of an iterative algorithm → cache with MEMORY_AND_DISK
  - One-shot ETL → don't cache; it wastes memory
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-caching")

    # Build an expensive-to-compute intermediate DF (multi-join + aggregation)
    def build_expensive_df():
        orders = spark.read.parquet(str(data_dir / "orders"))
        items = spark.read.parquet(str(data_dir / "order_items"))
        products = spark.read.parquet(str(data_dir / "products"))

        return (
            orders.filter(F.col("order_status").isin("paid", "shipped", "delivered"))
                .join(items, "order_id")
                .join(F.broadcast(products), "product_id")
                .groupBy("customer_id", "category")
                .agg(
                    F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
                    F.count("*").alias("item_count"),
                )
        )

    def workflow_without_cache():
        """DF is recomputed for each downstream query."""
        df = build_expensive_df()
        r1 = df.groupBy("category").agg(F.sum("revenue").alias("cat_rev")).collect()
        r2 = df.groupBy("customer_id").agg(F.sum("revenue").alias("cust_rev")).count()
        r3 = df.filter(F.col("revenue") > 1000).count()
        return len(r1) + r2 + r3

    def workflow_with_cache():
        """DF is computed once, reused 3 times."""
        df = build_expensive_df().persist(StorageLevel.MEMORY_AND_DISK)
        df.count()  # force materialization
        r1 = df.groupBy("category").agg(F.sum("revenue").alias("cat_rev")).collect()
        r2 = df.groupBy("customer_id").agg(F.sum("revenue").alias("cust_rev")).count()
        r3 = df.filter(F.col("revenue") > 1000).count()
        df.unpersist()
        return len(r1) + r2 + r3

    bench = Benchmark(spark, "caching", iterations=3, warmup=1)
    before = bench.run("recompute_each_time", workflow_without_cache)
    after = bench.run("cache_and_reuse", workflow_with_cache)

    print(compare(before, after))
    print()
    print("Rule: cache if the DF is used 2+ times AND the expensive work")
    print("      (joins, aggregations, UDFs) happens BEFORE the split point.")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
