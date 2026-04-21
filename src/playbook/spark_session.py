"""Spark session factories — baseline vs optimized.

Exposing two factories makes benchmark code explicit about which config is
under test. No more "did I remember to set AQE?" — you pick the factory,
config is set.
"""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession

DEFAULT_APP = "playbook"
DEFAULT_MASTER = "local[*]"

# Delta Lake requires specific jars + configs for tables to work
DELTA_CONFIGS: dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def get_spark(
    app_name: str = DEFAULT_APP,
    master: str = DEFAULT_MASTER,
    extra_configs: dict[str, Any] | None = None,
    enable_delta: bool = True,
) -> SparkSession:
    """Baseline Spark session — no perf tuning beyond defaults.

    Use this as the "before" in benchmarks.
    """
    configs = {
        "spark.driver.memory": "2g",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "200",  # default value, explicit
        # Disable AQE so benchmarks can measure its impact
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        "spark.sql.warehouse.dir": os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse"),
    }
    if enable_delta:
        configs.update(DELTA_CONFIGS)
    if extra_configs:
        configs.update(extra_configs)

    builder = SparkSession.builder.appName(app_name).master(master)
    for k, v in configs.items():
        builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_optimized_spark(
    app_name: str = DEFAULT_APP,
    master: str = DEFAULT_MASTER,
    extra_configs: dict[str, Any] | None = None,
    enable_delta: bool = True,
) -> SparkSession:
    """Optimized Spark session — AQE + cost-based optimizer + sensible shuffle partitions.

    Use this as the "after" in benchmarks.

    Key tuning:
      - AQE: runtime stats-based optimization (coalesce, skew, broadcast promotion)
      - shuffle.partitions = 2 * cores (vs. 200 default that fragments small jobs)
      - Kryo serializer (10-30% faster than Java default)
      - CBO + histogram statistics (better join ordering)
    """
    cores = os.cpu_count() or 4
    configs = {
        "spark.driver.memory": "4g",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": str(cores * 2),

        # Adaptive Query Execution — huge win with near-zero effort
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "1MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64MB",

        # Cost-based optimizer with stats
        "spark.sql.cbo.enabled": "true",
        "spark.sql.cbo.joinReorder.enabled": "true",
        "spark.sql.statistics.histogram.enabled": "true",

        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",

        # Broadcast threshold: 100MB (default 10MB is too conservative for modern clusters)
        "spark.sql.autoBroadcastJoinThreshold": "104857600",

        "spark.sql.warehouse.dir": os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse"),
    }
    if enable_delta:
        configs.update(DELTA_CONFIGS)
    if extra_configs:
        configs.update(extra_configs)

    builder = SparkSession.builder.appName(app_name).master(master)
    for k, v in configs.items():
        builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
