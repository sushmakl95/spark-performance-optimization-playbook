"""Benchmark: Adaptive Query Execution (AQE) impact.

The question: what does AQE actually do for you?

AQE (Spark 3.0+) makes three runtime optimizations using post-shuffle statistics:
  1. **Coalesce post-shuffle partitions**: fixes the "200-small-tasks" problem
  2. **Convert SMJ → BHJ at runtime**: promotes to broadcast if one side turns
     out small after filters
  3. **Handle skewed joins**: splits skewed partitions into sub-partitions

You enable AQE with one config. The benchmark here shows the impact on a
workload that hits all three cases.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark, get_spark


def benchmark(data_dir: Path) -> None:
    orders_path = str(data_dir / "orders")
    customers_path = str(data_dir / "customers")
    products_path = str(data_dir / "products")
    items_path = str(data_dir / "order_items")

    # Realistic multi-join query that benefits from all three AQE features
    def run_query(spark):
        orders = spark.read.parquet(orders_path)
        customers = spark.read.parquet(customers_path)
        products = spark.read.parquet(products_path)
        items = spark.read.parquet(items_path)

        return (
            orders.filter(F.col("order_status") == "delivered")
                .join(customers, "customer_id")
                .join(items, "order_id")
                .join(products, "product_id")
                .groupBy("country", "category")
                .agg(
                    F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                )
                .orderBy(F.desc("revenue"))
                .collect()
        )

    # Baseline: AQE off
    spark_no_aqe = get_spark(app_name="bench-aqe-off")
    bench_a = Benchmark(spark_no_aqe, "aqe_off", iterations=2, warmup=1)
    before = bench_a.run("no_aqe", lambda: len(run_query(spark_no_aqe)))
    spark_no_aqe.stop()

    # With AQE
    spark_aqe = get_optimized_spark(app_name="bench-aqe-on")
    bench_b = Benchmark(spark_aqe, "aqe_on", iterations=2, warmup=1)
    after = bench_b.run("aqe_enabled", lambda: len(run_query(spark_aqe)))
    spark_aqe.stop()

    print(compare(before, after))
    print()
    print("AQE features enabled:")
    print("  - Dynamic shuffle partition coalesce (fewer, larger tasks)")
    print("  - Runtime BHJ promotion (SMJ → BHJ when one side is small)")
    print("  - Skew join handling (splits hot partitions)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
