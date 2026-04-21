# Contributing

Contributions welcome — especially new benchmarks or clearer explanations.

## Adding a new benchmark

1. Create `benchmarks/bench_<technique>.py`
2. Template:

   ```python
   """Benchmark: <technique>.

   The question this answers: <problem in plain English>
   Setup: <datasets used>
   Expected: <before vs after>
   Why this matters: <real-world relevance>
   """
   from __future__ import annotations
   import argparse
   from pathlib import Path
   from pyspark.sql import functions as F
   from playbook.benchmark import Benchmark, compare
   from playbook.spark_session import get_optimized_spark

   def benchmark(data_dir: Path) -> None:
       spark = get_optimized_spark("bench-<technique>")
       # ...
       bench = Benchmark(spark, "<technique>", iterations=3, warmup=1)
       before = bench.run("baseline", lambda: ...)
       after  = bench.run("optimized", lambda: ...)
       print(compare(before, after))
       spark.stop()

   def main() -> None:
       parser = argparse.ArgumentParser()
       parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
       args = parser.parse_args()
       benchmark(args.data_dir)

   if __name__ == "__main__":
       main()
   ```

3. Register in `benchmarks/run_bench.py`:

   ```python
   BENCH_REGISTRY = {
       # ...
       "<technique>": "benchmarks.bench_<technique>",
   }
   ```

4. Add a Makefile shortcut (optional):

   ```makefile
   bench-<technique>:
       $(VENV_BIN)/python -m benchmarks.run_bench --technique <technique>
   ```

5. Update `docs/DECISION_TREE.md` if the technique fits the flow.

6. Include in the README's capability table.

## Style

- `ruff format` + `ruff check`
- Full type hints on public functions
- Docstrings on every benchmark explaining **what**, **why**, and **expected result**
- Avoid UDFs in benchmarks — they obscure what's being measured

## PR checklist

- [ ] New benchmark runs cleanly on 1x scale
- [ ] Docstring follows the template above
- [ ] Added to `BENCH_REGISTRY`
- [ ] `make ci` green locally
- [ ] No secrets, keys, or credentials in diff

## Tests

The harness itself is tested in `tests/unit/`. Benchmarks themselves don't need unit tests — they're small and directly observable.

If you refactor the `playbook/benchmark.py` or `playbook/spark_session.py` internals, add a test covering the change.

## Questions?

Open a [discussion](https://github.com/sushmakl95/spark-performance-optimization-playbook/discussions) or ping me on [LinkedIn](https://www.linkedin.com/in/sushmakl1995/).
