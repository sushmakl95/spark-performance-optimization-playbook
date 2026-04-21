"""Benchmark: Small-files problem and coalesce strategy.

The question: how badly does writing 1000 tiny Parquet files hurt vs 10
correctly-sized files?

Setup: same dataframe written two ways:
  - bad:   df.repartition(1000).write.parquet(...)  — 1000 files, ~50KB each
  - good:  df.coalesce(10).write.parquet(...)       — 10 files, ~5MB each
Then read each back and run a simple aggregation.

Why small files hurt:
  1. Listing cost on S3/HDFS — each file is a separate LIST + HEAD call
  2. Task scheduling overhead — Spark creates one task per file
  3. Parquet metadata overhead — each file has a schema + footer (~20KB)
  4. No benefit: modern Parquet readers can split large files as needed

The right target size is 128-256MB per file on S3, ~1GB on local HDFS.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-file-sizing")

    # Write the same dataframe two ways
    source = spark.read.parquet(str(data_dir / "order_items"))

    bad_path = str(data_dir / "_bench" / "order_items_tiny_files")
    good_path = str(data_dir / "_bench" / "order_items_right_sized")

    if not Path(bad_path).exists():
        print("Writing 1000 tiny files...")
        source.repartition(1000).write.mode("overwrite").parquet(bad_path)

    if not Path(good_path).exists():
        print("Writing 10 right-sized files...")
        source.coalesce(10).write.mode("overwrite").parquet(good_path)

    # Same query on both
    def query_path(path: str):
        return (
            spark.read.parquet(path)
                .groupBy("product_id")
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
                .orderBy(F.desc("revenue"))
                .limit(10)
                .collect()
        )

    bench = Benchmark(spark, "file_sizing", iterations=3, warmup=1)
    before = bench.run("1000_tiny_files", lambda: len(query_path(bad_path)))
    after = bench.run("10_right_sized", lambda: len(query_path(good_path)))

    print(compare(before, after))

    # Show the file counts to make it concrete
    def count_parquet(p):
        return sum(1 for f in Path(p).rglob("*.parquet"))
    print("\nFile counts:")
    print(f"  bad:  {count_parquet(bad_path)} files")
    print(f"  good: {count_parquet(good_path)} files")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
