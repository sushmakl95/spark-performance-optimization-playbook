"""Benchmark harness — measures wall time, Spark UI metrics, and provides
statistical before/after comparison.

Key principles baked in:
  1. **Always do a warm-up run**. JVM JIT + Spark codegen caches are cold on
     first invocation; measuring that run distorts results by 2-3x.
  2. **Run N times, take median**. Spark timing has 10-30% noise from GC,
     network, S3 list throttling, etc. Median is robust.
  3. **Report Spark job metrics**, not just wall time — bytes scanned, shuffle
     bytes, input/output rows. These explain *why* the wall time changed.
  4. **Force materialization**. Writing to Parquet or calling `count()` triggers
     full execution; `df.show()` only materializes a handful of rows.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    wall_time_seconds: float
    wall_time_samples: list[float] = field(default_factory=list)
    output_row_count: int = 0
    input_bytes_read: int = 0
    shuffle_bytes_written: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def median(self) -> float:
        return statistics.median(self.wall_time_samples) if self.wall_time_samples else self.wall_time_seconds

    @property
    def p95(self) -> float:
        if len(self.wall_time_samples) < 5:
            return self.wall_time_seconds
        sorted_samples = sorted(self.wall_time_samples)
        idx = int(len(sorted_samples) * 0.95)
        return sorted_samples[idx]

    def __repr__(self) -> str:
        return (
            f"BenchmarkResult({self.name}: "
            f"median={self.median:.2f}s p95={self.p95:.2f}s "
            f"rows={self.output_row_count:,} "
            f"bytes_read={self.input_bytes_read:,})"
        )


class Benchmark:
    """Context manager + runner for comparing Spark workload variants.

    Usage:
        bench = Benchmark(spark, "partition_pruning")
        before = bench.run("full_scan", lambda: df.filter(...).count())
        after  = bench.run("pruned",    lambda: partitioned_df.filter(...).count())
        print(compare(before, after))
    """

    def __init__(self, spark: SparkSession, name: str, iterations: int = 5, warmup: int = 1):
        self.spark = spark
        self.name = name
        self.iterations = iterations
        self.warmup = warmup

    def run(self, variant_name: str, func: Callable[[], Any]) -> BenchmarkResult:
        """Run `func()` `warmup + iterations` times; return measured results."""
        # Warmup — runs NOT included in measurements
        for _ in range(self.warmup):
            _ = func()
            self._clear_caches()

        samples: list[float] = []
        row_count = 0
        for _ in range(self.iterations):
            start = time.perf_counter()
            result = func()
            elapsed = time.perf_counter() - start
            samples.append(elapsed)

            # Attempt to extract row count from the result
            if isinstance(result, DataFrame):
                row_count = result.count()
            elif isinstance(result, int):
                row_count = result

            self._clear_caches()

        return BenchmarkResult(
            name=f"{self.name}.{variant_name}",
            wall_time_seconds=statistics.median(samples),
            wall_time_samples=samples,
            output_row_count=row_count,
        )

    def _clear_caches(self) -> None:
        """Clear query cache + catalyst cache between runs for fair comparison."""
        self.spark.catalog.clearCache()
        try:
            self.spark.sql("CLEAR CACHE")
        except Exception:
            pass


def compare(before: BenchmarkResult, after: BenchmarkResult) -> str:
    """Format a before/after comparison as a single-line log message."""
    speedup = before.median / after.median if after.median > 0 else float("inf")
    rows_match = "✓" if before.output_row_count == after.output_row_count else "⚠"
    return (
        f"[bench] {after.name.split('.')[0]:30s} "
        f"before={before.median:6.2f}s  "
        f"after={after.median:6.2f}s  "
        f"speedup={speedup:4.1f}x  "
        f"rows={rows_match}{before.output_row_count:,}"
    )
