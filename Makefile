.PHONY: help install install-dev seed-data bench-all bench lint format typecheck security test test-unit clean all

PYTHON := python3.11
VENV := .venv
VENV_BIN := $(VENV)/bin

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip setuptools wheel

install: $(VENV)/bin/activate  ## Install runtime deps (PySpark + Delta)
	$(VENV_BIN)/pip install -e .

install-dev: $(VENV)/bin/activate  ## Install dev + test + jupyter deps
	$(VENV_BIN)/pip install -e ".[dev]"
	$(VENV_BIN)/pre-commit install

seed-data:  ## Generate 1x synthetic dataset (~50MB, ~30s)
	$(VENV_BIN)/python -m data.generators.ecommerce --output-dir data/generated --scale 1x

seed-data-10x:  ## Generate 10x dataset (~500MB, ~5min)
	$(VENV_BIN)/python -m data.generators.ecommerce --output-dir data/generated --scale 10x

seed-data-skewed:  ## Generate 10x dataset with customer skew (for skew benchmarks)
	$(VENV_BIN)/python -m data.generators.ecommerce --output-dir data/generated --scale 10x --skewed

bench-all:  ## Run every benchmark end-to-end
	$(VENV_BIN)/python -m benchmarks.run_bench --technique all

bench-partition:  ## Run partition_pruning benchmark
	$(VENV_BIN)/python -m benchmarks.run_bench --technique partition_pruning

bench-broadcast:  ## Run broadcast_join benchmark
	$(VENV_BIN)/python -m benchmarks.run_bench --technique broadcast_join

bench-skew:  ## Run skew_handling benchmark
	$(VENV_BIN)/python -m benchmarks.run_bench --technique skew_handling

bench-aqe:  ## Run aqe benchmark
	$(VENV_BIN)/python -m benchmarks.run_bench --technique aqe

bench-zorder:  ## Run zorder benchmark
	$(VENV_BIN)/python -m benchmarks.run_bench --technique zorder

test: test-unit  ## Run full test suite

test-unit:  ## Fast unit tests (benchmark harness logic)
	$(VENV_BIN)/pytest tests/unit -v -m unit

lint:  ## Run ruff
	$(VENV_BIN)/ruff check src tests benchmarks scripts

format:  ## Auto-format
	$(VENV_BIN)/ruff format src tests benchmarks scripts
	$(VENV_BIN)/ruff check --fix src tests benchmarks scripts

typecheck:  ## Run mypy
	$(VENV_BIN)/mypy src --ignore-missing-imports

security:  ## Run bandit
	$(VENV_BIN)/bandit -c pyproject.toml -r src benchmarks scripts -lll

cluster-up:  ## Start Docker Spark cluster
	docker compose -f compose/docker-compose.yml up -d
	@echo "Spark Master UI: http://localhost:8080"

cluster-down:  ## Stop Docker Spark cluster
	docker compose -f compose/docker-compose.yml down

jupyter:  ## Start Jupyter Lab
	$(VENV_BIN)/jupyter lab

clean:  ## Clean spark + pyspark temp files
	rm -rf spark-warehouse/ metastore_db/ derby.log *.log
	rm -rf .pytest_cache/ .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-data:  ## Clean generated synthetic data (destructive)
	rm -rf data/generated

all: install-dev lint typecheck security test  ## Full CI simulation locally
