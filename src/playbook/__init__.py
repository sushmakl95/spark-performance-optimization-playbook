"""Spark Performance Optimization Playbook — reusable utilities."""

from playbook.benchmark import Benchmark, BenchmarkResult, compare
from playbook.spark_session import get_optimized_spark, get_spark

__all__ = ["Benchmark", "BenchmarkResult", "compare", "get_optimized_spark", "get_spark"]
