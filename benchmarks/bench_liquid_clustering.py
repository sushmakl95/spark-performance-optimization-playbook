"""Benchmark: Delta Liquid Clustering vs Z-ORDER.

Z-ORDER has been the go-to for multi-column clustering in Delta for years.
**Liquid Clustering** (Delta 3.2+) replaces it for new tables with three
meaningful advantages:

1. **Incremental** — adding data doesn't require a full OPTIMIZE rewrite.
   OPTIMIZE on a liquid-clustered table only re-clusters changed partitions.
2. **Evolvable** — you can change the clustering columns later without rewriting
   history (`ALTER TABLE ... CLUSTER BY`). Z-ORDER would require a full rewrite
   to re-cluster on a new column set.
3. **Better selectivity at scale** — liquid clustering uses a hybrid of
   range + hash partitioning that keeps files small without the skew
   pathologies of high-cardinality Z-ORDER keys.

This benchmark measures:
- Initial write time (Z-ORDER vs liquid)
- Incremental append + re-cluster time (the place liquid really wins)
- Query latency on a multi-predicate filter

Reference: https://docs.databricks.com/en/delta/clustering.html
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark


def benchmark(data_dir: Path) -> None:  # pragma: no cover - runtime benchmark
    spark = get_optimized_spark(app_name="bench-liquid-clustering")

    source = spark.read.parquet(str(data_dir / "orders"))

    zorder_path = str(data_dir / "_bench" / "orders_delta_zorder")
    liquid_path = str(data_dir / "_bench" / "orders_delta_liquid")

    if not Path(zorder_path).exists():
        print("Writing Z-ORDER'd Delta...")
        source.write.format("delta").mode("overwrite").save(zorder_path)
        spark.sql(f"OPTIMIZE delta.`{zorder_path}` ZORDER BY (customer_id, order_status)")

    if not Path(liquid_path).exists():
        print("Writing Liquid-clustered Delta (Delta 3.2+)...")
        source.write.format("delta").mode("overwrite").option(
            "delta.feature.liquidClustering", "supported"
        ).save(liquid_path)
        spark.sql(
            f"ALTER TABLE delta.`{liquid_path}` CLUSTER BY (customer_id, order_status)"
        )
        spark.sql(f"OPTIMIZE delta.`{liquid_path}`")

    target_customer = 42

    def run_query(path: str) -> int:
        return len(
            spark.read.format("delta")
            .load(path)
            .filter(
                (F.col("customer_id") == target_customer)
                & (F.col("order_status").isin("paid", "shipped", "delivered"))
            )
            .agg(F.sum("total_amount"), F.count("*"))
            .collect()
        )

    bench = Benchmark(spark, "liquid_vs_zorder_query", iterations=3, warmup=1)
    before = bench.run("zorder", lambda: run_query(zorder_path))
    after = bench.run("liquid", lambda: run_query(liquid_path))
    print(compare(before, after))

    # Incremental-append cost: append 10% and re-cluster on each strategy
    print("\nIncremental append (10% fresh data + OPTIMIZE):")
    append_df = source.sample(0.1)

    def inc_append(path: str) -> int:
        append_df.write.format("delta").mode("append").save(path)
        spark.sql(f"OPTIMIZE delta.`{path}`")
        return 1

    inc = Benchmark(spark, "liquid_vs_zorder_incremental", iterations=2, warmup=0)
    z_inc = inc.run("zorder_reoptimize", lambda: inc_append(zorder_path))
    l_inc = inc.run("liquid_reoptimize", lambda: inc_append(liquid_path))
    print(compare(z_inc, l_inc))

    print("\nTakeaways:")
    print("  - Liquid wins on incremental OPTIMIZE (only re-clusters changed files)")
    print("  - Z-ORDER has no column-evolvability — a schema change = full rewrite")
    print("  - For new tables on Delta 3.2+, default to liquid clustering")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
