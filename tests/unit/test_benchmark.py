"""Unit tests for the benchmark harness."""

from __future__ import annotations

import pytest

from playbook.benchmark import Benchmark, BenchmarkResult, compare

pytestmark = pytest.mark.unit


def test_benchmark_result_median():
    result = BenchmarkResult(
        name="test",
        wall_time_seconds=0.0,
        wall_time_samples=[1.0, 2.0, 3.0, 4.0, 5.0],
    )
    assert result.median == 3.0


def test_benchmark_result_p95_with_few_samples():
    # P95 needs >=5 samples; falls back to wall_time_seconds otherwise
    result = BenchmarkResult(name="test", wall_time_seconds=2.5, wall_time_samples=[1.0, 2.0, 3.0])
    assert result.p95 == 2.5


def test_benchmark_runs_func_multiple_times(spark):
    counter = {"calls": 0}

    def work():
        counter["calls"] += 1
        return spark.range(10).count()

    bench = Benchmark(spark, "test", iterations=3, warmup=1)
    result = bench.run("variant_a", work)

    assert counter["calls"] == 4  # 1 warmup + 3 iterations
    assert len(result.wall_time_samples) == 3
    assert result.output_row_count == 10


def test_compare_formats_speedup():
    before = BenchmarkResult(name="test.before", wall_time_seconds=10.0, wall_time_samples=[10.0])
    after = BenchmarkResult(name="test.after", wall_time_seconds=2.0, wall_time_samples=[2.0])
    output = compare(before, after)
    assert "speedup=" in output
    assert "5.0x" in output
