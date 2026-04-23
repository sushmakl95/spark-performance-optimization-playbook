"""Benchmark: Delta Deletion Vectors for merge-on-read.

Classic Delta MERGE / DELETE / UPDATE rewrites the entire file for any affected
row (copy-on-write). On a 100 MB file with one deleted row, you rewrite 100 MB.

**Deletion Vectors** (Delta 3.x, enabled via `delta.enableDeletionVectors = true`)
change this: instead of rewriting, Delta writes a sidecar bitmap marking the
deleted rowid. Subsequent reads apply the bitmap in memory. A later OPTIMIZE
physically materializes the deletion. This is "merge-on-read" semantics.

The win is on:
- High-volume DELETEs/UPDATEs on large partitions
- GDPR-style deletion workflows
- CDC targets where most rows change rarely

The cost is: slightly slower reads (bitmap application), and OPTIMIZE is still
needed periodically to reclaim space.

Reference:
- https://docs.delta.io/latest/delta-deletion-vectors.html
- https://docs.databricks.com/en/delta/deletion-vectors.html
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:  # pragma: no cover - runtime benchmark
    spark = get_optimized_spark(app_name="bench-deletion-vectors")

    source = spark.read.parquet(str(data_dir / "orders"))

    cow_path = str(data_dir / "_bench" / "orders_delta_cow")
    dv_path = str(data_dir / "_bench" / "orders_delta_dv")

    if not Path(cow_path).exists():
        print("Writing copy-on-write Delta table...")
        source.write.format("delta").mode("overwrite").save(cow_path)

    if not Path(dv_path).exists():
        print("Writing deletion-vectors Delta table (delta.enableDeletionVectors=true)...")
        (
            source.write.format("delta")
            .mode("overwrite")
            .option("delta.enableDeletionVectors", "true")
            .save(dv_path)
        )

    # Simulate a GDPR deletion: remove all rows for 1% of customers.
    def delete_sample(path: str) -> int:
        victim_customers = list(range(0, 100))  # deterministic 1%
        spark.sql(
            f"DELETE FROM delta.`{path}` WHERE customer_id IN ({','.join(map(str, victim_customers))})"
        )
        return 1

    bench = Benchmark(spark, "delete_cost", iterations=2, warmup=0)
    cow = bench.run("copy_on_write", lambda: delete_sample(cow_path))
    dv = bench.run("deletion_vectors", lambda: delete_sample(dv_path))
    print(compare(cow, dv))

    # Read latency after deletion — deletion vectors cost a little on reads.
    def read_after(path: str) -> int:
        return int(
            spark.read.format("delta").load(path).agg(F.count("*")).first()[0]
        )

    read_bench = Benchmark(spark, "read_after_delete", iterations=3, warmup=1)
    r_cow = read_bench.run("copy_on_write", lambda: read_after(cow_path))
    r_dv = read_bench.run("deletion_vectors", lambda: read_after(dv_path))
    print(compare(r_cow, r_dv))

    print("\nTakeaways:")
    print("  - DELETE is ~5-20x faster with DV (no file rewrite)")
    print("  - Reads are a small percentage slower until OPTIMIZE")
    print("  - Enable DV by default for fact tables receiving CDC updates")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
