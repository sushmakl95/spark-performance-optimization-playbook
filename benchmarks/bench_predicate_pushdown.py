"""Benchmark: Predicate pushdown into Parquet readers.

The question: how much does Spark's ability to push filters INTO the Parquet
reader (via column min/max stats) save vs filtering after loading?

How it works: Parquet files store column statistics (min, max, null_count)
per row-group (~128MB). Spark's reader can SKIP entire row-groups whose stats
prove they can't match the predicate — without decoding the data.

Effective predicates:
  - equality on low-cardinality columns (category, status)
  - range queries on sorted columns (dates, timestamps, IDs)

Ineffective predicates:
  - LIKE patterns
  - UDF results
  - functions like UPPER(), TRIM() — the reader sees the raw value
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-predicate-pushdown")

    items_path = str(data_dir / "order_items")

    def query_pushdown_effective():
        """
        Simple equality — Spark can push into Parquet reader.
        Look for `PushedFilters: [EqualTo(product_id, 42)]` in explain().
        """
        return (
            spark.read.parquet(items_path)
                .filter(F.col("product_id") == 42)
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("rev"))
                .collect()
        )

    def query_pushdown_ineffective():
        """
        Same logical result, but wrapping in a function defeats pushdown.
        Spark has to read the raw value, compute ABS(), THEN filter.
        """
        return (
            spark.read.parquet(items_path)
                .filter(F.abs(F.col("product_id") - 42) < 1)  # equivalent to == 42
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("rev"))
                .collect()
        )

    bench = Benchmark(spark, "predicate_pushdown", iterations=3, warmup=1)
    before = bench.run("function_blocks_pushdown", lambda: len(query_pushdown_ineffective()))
    after = bench.run("direct_comparison", lambda: len(query_pushdown_effective()))

    print(compare(before, after))

    print("\n--- PUSHED plan (look for 'PushedFilters' in DataFilters) ---")
    spark.read.parquet(items_path).filter(F.col("product_id") == 42).explain()

    print("\n--- NOT-PUSHED plan (filter applied after scan) ---")
    spark.read.parquet(items_path).filter(F.abs(F.col("product_id") - 42) < 1).explain()

    print()
    print("Rule: avoid wrapping columns in functions inside WHERE clauses.")
    print("      `WHERE upper(country) = 'US'` defeats pushdown;")
    print("      write `WHERE country = 'us'` after normalizing at ingestion.")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
