"""Benchmark: Broadcast join vs shuffle join.

The question this answers: when one side of a join is "small", how much does
broadcasting it save vs a regular shuffle sort-merge join?

Setup:
  - orders (large): 1M rows, ~100MB
  - products (small): 5k rows, ~500KB

Query: orders JOIN products ON product_id

Expected:
  - Shuffle join: both sides shuffled — writes ~100MB shuffle files
  - Broadcast join: products replicated to every executor — zero shuffle

The internals: Spark's default auto-broadcast threshold is 10MB. Many real
dims are 20-50MB (category lookups, region lookups, date dim). Bumping to
100MB is almost always a win.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(
        app_name="bench-broadcast-join",
        # Disable AQE — we want a *controlled* comparison of broadcast hint vs no hint
        extra_configs={"spark.sql.adaptive.enabled": "false"},
    )

    # Need order_items as the "large" side for a meaningful shuffle
    order_items = spark.read.parquet(str(data_dir / "order_items"))
    products = spark.read.parquet(str(data_dir / "products"))

    def shuffle_join_query():
        # Force sort-merge shuffle by disabling the auto-broadcast threshold
        return (
            order_items.join(products, "product_id", "inner")
                .groupBy("category")
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
                .collect()
        )

    def broadcast_join_query():
        return (
            order_items.join(F.broadcast(products), "product_id", "inner")
                .groupBy("category")
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
                .collect()
        )

    # Force shuffle by setting threshold to 0 for the "before" run
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    bench = Benchmark(spark, "broadcast_join", iterations=3, warmup=1)
    before = bench.run("shuffle", lambda: len(shuffle_join_query()))

    # Re-enable broadcast for the "after" — but use explicit hint to be unambiguous
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    after = bench.run("broadcast", lambda: len(broadcast_join_query()))

    print(compare(before, after))

    print("\n--- SHUFFLE JOIN PLAN (note 'SortMergeJoin') ---")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    order_items.join(products, "product_id", "inner").explain()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))

    print("\n--- BROADCAST JOIN PLAN (note 'BroadcastHashJoin') ---")
    order_items.join(F.broadcast(products), "product_id", "inner").explain()

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
