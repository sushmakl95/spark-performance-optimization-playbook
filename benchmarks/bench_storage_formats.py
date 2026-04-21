"""Benchmark: Storage format comparison (Parquet vs ORC vs Delta).

The question: does format choice matter for read-heavy analytical queries?

Short answer: for single-file reads, all three columnar formats are within
10% of each other. The bigger differences show up in:
  - Write path (Delta's ACID overhead vs raw Parquet)
  - Small-update workloads (Delta excels with its MERGE primitive)
  - Time-travel / audit requirements (Delta + Iceberg only)

Here we compare read performance on the same logical dataset, re-serialized
into three formats.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:
    spark = get_optimized_spark(app_name="bench-storage-formats")

    source = spark.read.parquet(str(data_dir / "order_items"))

    parquet_path = str(data_dir / "_bench" / "items_parquet")
    orc_path = str(data_dir / "_bench" / "items_orc")
    delta_path = str(data_dir / "_bench" / "items_delta")

    # Write all three formats if not already cached
    if not Path(parquet_path).exists():
        print("Writing parquet...")
        source.write.mode("overwrite").parquet(parquet_path)
    if not Path(orc_path).exists():
        print("Writing orc...")
        source.write.mode("overwrite").orc(orc_path)
    if not Path(delta_path).exists():
        print("Writing delta...")
        source.write.format("delta").mode("overwrite").save(delta_path)

    # Same analytical query on each
    def run_query(reader):
        return (
            reader.filter(F.col("quantity") > 2)
                .groupBy("product_id")
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("rev"))
                .orderBy(F.desc("rev"))
                .limit(10)
                .collect()
        )

    bench = Benchmark(spark, "storage_formats", iterations=3, warmup=1)

    parquet_result = bench.run("parquet", lambda: len(run_query(spark.read.parquet(parquet_path))))
    orc_result = bench.run("orc", lambda: len(run_query(spark.read.orc(orc_path))))
    delta_result = bench.run("delta", lambda: len(run_query(spark.read.format("delta").load(delta_path))))

    print()
    print(f"parquet: {parquet_result.median:.2f}s")
    print(f"orc:     {orc_result.median:.2f}s")
    print(f"delta:   {delta_result.median:.2f}s")
    print()
    # Show comparison against parquet as baseline
    print(compare(parquet_result, orc_result))
    print(compare(parquet_result, delta_result))
    print()
    print("Takeaways:")
    print("  - Parquet/ORC nearly identical on read. Pick whichever matches")
    print("    your ecosystem (AWS prefers Parquet, Hive legacy prefers ORC).")
    print("  - Delta's read overhead is small; its value is the write path")
    print("    (MERGE, schema evolution, time travel).")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
