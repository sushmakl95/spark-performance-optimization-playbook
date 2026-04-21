"""Benchmark: Executor count and memory sizing.

The question: on a fixed budget of cores x memory, does it help more to have
  (a) MANY small executors, or
  (b) FEW large executors?

The answer: it depends, but there's a rule of thumb that covers 80% of cases:
  - 4-5 cores per executor
  - 4-8 GB memory per executor
  - Leave 1 core + 1 GB for the OS/YARN overhead

Why: too many small executors = overhead (network, class loading, codegen
cache duplication). Too few huge executors = GC pauses + inefficient memory
use (JVM heap inefficiency grows superlinearly past ~32GB).

This benchmark shows the difference in local mode by toggling parallelism.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import functions as F

from playbook.benchmark import Benchmark, compare
from playbook.spark_session import get_spark


def benchmark(data_dir: Path) -> None:
    cores = os.cpu_count() or 4

    # Variant A: too-small "executors" (1 thread each)
    spark_small = get_spark(
        app_name="bench-executor-small",
        master="local[1]",  # single thread = worst case
        enable_delta=False,
    )

    def run_query(spark):
        return (
            spark.read.parquet(str(data_dir / "order_items"))
                .join(
                    spark.read.parquet(str(data_dir / "orders")).select("order_id", "customer_id", "order_status"),
                    "order_id",
                )
                .filter(F.col("order_status") == "delivered")
                .groupBy("customer_id")
                .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("rev"))
                .collect()
        )

    bench_a = Benchmark(spark_small, "exec_small", iterations=2, warmup=1)
    before = bench_a.run("local_1_thread", lambda: len(run_query(spark_small)))
    spark_small.stop()

    # Variant B: right-sized parallelism (cores - 1)
    spark_right = get_spark(
        app_name="bench-executor-right",
        master=f"local[{max(2, cores - 1)}]",
        enable_delta=False,
    )

    bench_b = Benchmark(spark_right, "exec_right", iterations=2, warmup=1)
    after = bench_b.run(f"local_{max(2, cores - 1)}_threads", lambda: len(run_query(spark_right)))
    spark_right.stop()

    print(compare(before, after))
    print()
    print(f"Detected {cores} cores. Rules:")
    print("  1. 4-5 cores per executor (balances CPU + IPC overhead)")
    print("  2. 4-8 GB memory per executor (avoids GC pauses)")
    print("  3. Leave 1 core + 1 GB for OS/yarn overhead")
    print("  4. Total executors = cores / 5, then round down")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    benchmark(args.data_dir)


if __name__ == "__main__":
    main()
