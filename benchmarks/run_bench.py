"""`spark-bench` — unified CLI for running benchmarks."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

BENCH_REGISTRY = {
    "file_sizing": "benchmarks.bench_file_sizing",
    "partition_pruning": "benchmarks.bench_partition_pruning",
    "broadcast_join": "benchmarks.bench_broadcast_join",
    "skew_handling": "benchmarks.bench_skew_handling",
    "aqe": "benchmarks.bench_aqe",
    "caching": "benchmarks.bench_caching",
    "predicate_pushdown": "benchmarks.bench_predicate_pushdown",
    "storage_formats": "benchmarks.bench_storage_formats",
    "zorder": "benchmarks.bench_zorder",
    "executor_tuning": "benchmarks.bench_executor_tuning",
}


def run_single(technique: str, data_dir: Path) -> int:
    module = BENCH_REGISTRY.get(technique)
    if not module:
        print(f"Unknown technique: {technique}", file=sys.stderr)
        print(f"Available: {', '.join(BENCH_REGISTRY)}", file=sys.stderr)
        return 1

    print(f"\n{'=' * 70}")
    print(f"  Running: {technique}")
    print(f"{'=' * 70}\n")
    return subprocess.call([sys.executable, "-m", module, "--data-dir", str(data_dir)])


def run_all(data_dir: Path) -> int:
    failures = []
    for tech in BENCH_REGISTRY:
        rc = run_single(tech, data_dir)
        if rc != 0:
            failures.append(tech)

    print(f"\n{'=' * 70}")
    if failures:
        print(f"  FAILED: {', '.join(failures)}")
        return 1
    print("  All benchmarks completed.")
    print(f"{'=' * 70}\n")
    return 0


def cli() -> None:
    parser = argparse.ArgumentParser(description="Run Spark performance benchmarks")
    parser.add_argument(
        "--technique",
        choices=["all", *BENCH_REGISTRY.keys()],
        default="all",
        help="Which benchmark to run (default: all)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/generated"),
        help="Path to generated dataset (run `make seed-data` first)",
    )
    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Data directory {args.data_dir} does not exist.", file=sys.stderr)
        print("Run: python -m data.generators.ecommerce --output-dir data/generated --scale 1x", file=sys.stderr)
        sys.exit(1)

    if args.technique == "all":
        sys.exit(run_all(args.data_dir))
    else:
        sys.exit(run_single(args.technique, args.data_dir))


if __name__ == "__main__":
    cli()
