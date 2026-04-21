"""Benchmark: Delta Z-ORDER for multi-dimensional filtering.

The question: when your queries filter on multiple columns (e.g.,
`WHERE customer_id = X AND order_date = Y`), how much does Z-ORDER help?

How Z-ORDER works: sorts data into "space-filling curves" so values that are
close together on MULTIPLE dimensions end up in the same file. Unlike
partitioning (works on 1 column), Z-ORDER works on 2-4 columns simultaneously.

This is the killer feature that makes Delta + Databricks 10-40x faster than
raw Parquet on multi-predicate queries.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-zorder")

    # Materialize as Delta so we can Z-ORDER it
    source = spark.read.parquet(str(data_dir / "orders"))

    baseline_path = str(data_dir / "_bench" / "orders_delta_baseline")
    zordered_path = str(data_dir / "_bench" / "orders_delta_zordered")

    if not Path(baseline_path).exists():
        print("Writing baseline Delta (no clustering)...")
        source.write.format("delta").mode("overwrite").save(baseline_path)

    if not Path(zordered_path).exists():
        print("Writing + Z-ORDER'd Delta (this takes longer on the write side)...")
        source.write.format("delta").mode("overwrite").save(zordered_path)
        spark.sql(f"OPTIMIZE delta.`{zordered_path}` ZORDER BY (customer_id, order_status)")

    # Multi-predicate query that benefits from Z-ORDER on (customer_id, order_status)
    target_customer = 42
    def run_query(path: str):
        return (
            spark.read.format("delta").load(path)
                .filter(
                    (F.col("customer_id") == target_customer)
                    & (F.col("order_status").isin("paid", "shipped", "delivered"))
                )
                .agg(F.sum("total_amount").alias("rev"), F.count("*").alias("n"))
                .collect()
        )

    bench = Benchmark(spark, "zorder_clustering", iterations=3, warmup=1)
    before = bench.run("no_clustering", lambda: len(run_query(baseline_path)))
    after = bench.run("zordered", lambda: len(run_query(zordered_path)))

    print(compare(before, after))
    print()
    print("Z-ORDER costs:")
    print("  - Extra write time (~2x vs plain Parquet)")
    print("  - Only useful when queries filter on clustered columns")
    print("  - Re-OPTIMIZE needed periodically as new data arrives")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
