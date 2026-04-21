"""Benchmark: Handling data skew via key salting.

The question this answers: when a join has hot keys that dwarf the rest, how
much does manual salting help?

Setup: skewed orders (30% of orders attributed to 0.1% of customers — classic
"power law" user distribution). Joining orders to customers on customer_id.

Without salting, one reducer gets ~300k rows while others get ~100 each → the
job is bottlenecked on that one reducer ("straggler").

Salting strategy:
  1. Append a random 0-9 suffix to the hot side's key: (customer_id, salt)
  2. Explode the cold side: each customer_id becomes 10 rows with salts 0-9
  3. Join on the composite key — now the hot customer's work spreads across 10 reducers

Trade-off: 10× the rows on the cold side. Only worth it when skew is severe.

Modern escape hatch: Spark 3.x AQE with skew-join handling does this automatically.
We benchmark both: manual salting vs AQE auto-skew.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_optimized_spark, get_spark


SALT_BUCKETS = 10


def benchmark(data_dir: Path) -> None:
    orders_path = str(data_dir / "orders")
    customers_path = str(data_dir / "customers")

    # ---- Variant A: baseline, NO AQE, no salting ----
    spark_baseline = get_spark(app_name="bench-skew-baseline", enable_delta=False)
    orders = spark_baseline.read.parquet(orders_path)
    customers = spark_baseline.read.parquet(customers_path)

    def baseline_join():
        return (
            orders.join(customers, "customer_id", "inner")
                .groupBy("country")
                .agg(F.sum("total_amount").alias("rev"))
                .collect()
        )

    bench_a = Benchmark(spark_baseline, "skew_baseline", iterations=2, warmup=1)
    before = bench_a.run("no_tuning", lambda: len(baseline_join()))
    spark_baseline.stop()

    # ---- Variant B: manual salting ----
    spark_salted = get_spark(
        app_name="bench-skew-salted",
        enable_delta=False,
        extra_configs={"spark.sql.adaptive.enabled": "false"},
    )
    orders = spark_salted.read.parquet(orders_path)
    customers = spark_salted.read.parquet(customers_path)

    # Salt the hot side (orders): add a random 0-(N-1) suffix to customer_id
    orders_salted = orders.withColumn(
        "salt", (F.rand() * SALT_BUCKETS).cast("int")
    )

    # Explode the cold side (customers): one row becomes SALT_BUCKETS rows
    salt_values = spark_salted.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
    customers_expanded = customers.crossJoin(salt_values)

    def salted_join():
        return (
            orders_salted.join(
                customers_expanded,
                on=[
                    orders_salted.customer_id == customers_expanded.customer_id,
                    orders_salted.salt == customers_expanded.salt,
                ],
                how="inner",
            )
            .drop(customers_expanded.customer_id)
            .drop(customers_expanded.salt)
            .drop(orders_salted.salt)
            .groupBy("country")
            .agg(F.sum("total_amount").alias("rev"))
            .collect()
        )

    bench_b = Benchmark(spark_salted, "skew_salted", iterations=2, warmup=1)
    after_salted = bench_b.run("manual_salt", lambda: len(salted_join()))
    spark_salted.stop()

    # ---- Variant C: AQE auto-skew (Spark 3.x) ----
    spark_aqe = get_optimized_spark(app_name="bench-skew-aqe", enable_delta=False)
    orders = spark_aqe.read.parquet(orders_path)
    customers = spark_aqe.read.parquet(customers_path)

    def aqe_join():
        return (
            orders.join(customers, "customer_id", "inner")
                .groupBy("country")
                .agg(F.sum("total_amount").alias("rev"))
                .collect()
        )

    bench_c = Benchmark(spark_aqe, "skew_aqe", iterations=2, warmup=1)
    after_aqe = bench_c.run("aqe_autoskew", lambda: len(aqe_join()))
    spark_aqe.stop()

    print()
    print(compare(before, after_salted))
    print(compare(before, after_aqe))
    print()
    print("Takeaway: if you're on Spark 3.0+, AQE auto-skew is usually good enough.")
    print("Manual salting is reserved for extreme skew (> 100x) or older Spark versions.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
