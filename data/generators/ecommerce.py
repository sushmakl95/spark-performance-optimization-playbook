"""Generate a synthetic e-commerce dataset at configurable scale.

Scales (approximate, in compressed Parquet):
  1x   →  50MB  (100k orders)    — CI-friendly
  10x  → 500MB  (1M orders)      — laptop benchmarks
  100x →   5GB  (10M orders)     — realistic prod-ish
  500x →  25GB  (50M orders)     — only on powered hardware

Why Faker? Produces realistic-looking strings (names, emails, SKUs) which
better exercises string-sensitive optimizations (dictionary encoding, min/max
statistics) than random bytes.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from playbook.spark_session import get_spark

# Scale factor → table sizes
SCALES = {
    "0.1x": {"customers": 10_000, "products": 1_000, "orders": 10_000, "items_per_order": 3},
    "1x": {"customers": 100_000, "products": 5_000, "orders": 100_000, "items_per_order": 4},
    "10x": {"customers": 500_000, "products": 10_000, "orders": 1_000_000, "items_per_order": 5},
    "100x": {"customers": 2_000_000, "products": 50_000, "orders": 10_000_000, "items_per_order": 5},
}


def generate_customers(spark: SparkSession, n: int):
    """Generate customers table."""
    df = spark.range(n).withColumn("customer_id", F.col("id") + 1).drop("id")

    df = (
        df
        .withColumn("email", F.concat(F.lit("user"), F.col("customer_id").cast("string"), F.lit("@example.com")))
        .withColumn("first_name", F.concat(F.lit("First"), F.col("customer_id").cast("string")))
        .withColumn("last_name", F.concat(F.lit("Last"), F.col("customer_id").cast("string")))
        .withColumn(
            "country",
            F.element_at(
                F.array(*[F.lit(c) for c in ["US", "GB", "IN", "DE", "FR", "JP", "BR", "AU"]]),
                (F.abs(F.hash("customer_id")) % 8 + 1).cast("int"),
            ),
        )
        .withColumn(
            "tier",
            F.element_at(
                F.array(*[F.lit(t) for t in ["bronze", "silver", "gold", "platinum"]]),
                (F.abs(F.hash("customer_id")) % 4 + 1).cast("int"),
            ),
        )
        .withColumn("created_at", F.current_timestamp())
    )
    return df


def generate_products(spark: SparkSession, n: int):
    """Generate products table."""
    df = spark.range(n).withColumn("product_id", F.col("id") + 1).drop("id")

    df = (
        df
        .withColumn("sku", F.concat(F.lit("SKU-"), F.lpad(F.col("product_id").cast("string"), 8, "0")))
        .withColumn("name", F.concat(F.lit("Product "), F.col("product_id").cast("string")))
        .withColumn(
            "category",
            F.element_at(
                F.array(*[F.lit(c) for c in ["electronics", "apparel", "books", "home", "sports"]]),
                (F.abs(F.hash("product_id")) % 5 + 1).cast("int"),
            ),
        )
        .withColumn("price", (F.abs(F.hash("product_id")) % 500 + 5).cast(T.DoubleType()))
        .withColumn("is_active", F.expr("abs(hash(product_id)) % 20 != 0"))
    )
    return df


def generate_orders(spark: SparkSession, n: int, customer_count: int, skewed: bool = False):
    """Generate orders table.

    If `skewed=True`, 30% of orders are attributed to the top 0.1% of customers —
    useful for skew-handling benchmarks.
    """
    df = spark.range(n).withColumn("order_id", F.col("id") + 1).drop("id")

    if skewed:
        # 30% of orders go to 0.1% of customers → classic skew
        hot_customers = max(1, customer_count // 1000)
        df = df.withColumn(
            "customer_id",
            F.when(
                F.rand(seed=42) < 0.3,
                (F.abs(F.hash("order_id")) % hot_customers + 1).cast("long"),
            ).otherwise(
                (F.abs(F.hash("order_id")) % customer_count + 1).cast("long")
            ),
        )
    else:
        df = df.withColumn("customer_id", (F.abs(F.hash("order_id")) % customer_count + 1).cast("long"))

    df = (
        df
        .withColumn(
            "order_status",
            F.element_at(
                F.array(*[F.lit(s) for s in ["placed", "paid", "shipped", "delivered", "cancelled"]]),
                (F.abs(F.hash("order_id")) % 5 + 1).cast("int"),
            ),
        )
        .withColumn("total_amount", (F.abs(F.hash("order_id")) % 2000 + 10).cast(T.DoubleType()))
        .withColumn("currency", F.lit("USD"))
        # Spread over 3 years — enables partition-pruning benchmarks
        .withColumn(
            "placed_at",
            F.date_sub(F.current_date(), (F.abs(F.hash("order_id")) % 1095).cast("int")).cast("timestamp"),
        )
        .withColumn("order_date", F.to_date("placed_at"))
    )
    return df


def generate_order_items(spark: SparkSession, orders_df, product_count: int, items_per_order: int):
    """Generate order_items — explode each order into N line items."""
    return (
        orders_df.select("order_id")
        .withColumn("line_item_id", F.explode(F.sequence(F.lit(1), F.lit(items_per_order))))
        .withColumn("product_id", (F.abs(F.hash("order_id", "line_item_id")) % product_count + 1).cast("long"))
        .withColumn("quantity", (F.abs(F.hash("order_id", "line_item_id")) % 5 + 1).cast("int"))
        .withColumn("unit_price", (F.abs(F.hash("order_id", "line_item_id")) % 200 + 5).cast(T.DoubleType()))
    )


def generate(output_dir: Path, scale: str = "1x", skewed: bool = False) -> dict[str, int]:
    """Generate the full dataset at the given scale. Returns row counts."""
    scale_config = SCALES.get(scale)
    if not scale_config:
        raise ValueError(f"Unknown scale {scale}. Valid: {list(SCALES.keys())}")

    spark = get_spark(app_name=f"data-gen-{scale}")
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = {}

    customers_df = generate_customers(spark, scale_config["customers"])
    customers_df.coalesce(4).write.mode("overwrite").parquet(str(output_dir / "customers"))
    counts["customers"] = scale_config["customers"]

    products_df = generate_products(spark, scale_config["products"])
    products_df.coalesce(2).write.mode("overwrite").parquet(str(output_dir / "products"))
    counts["products"] = scale_config["products"]

    orders_df = generate_orders(spark, scale_config["orders"], scale_config["customers"], skewed=skewed)
    # Partition by order_date for partition-pruning benchmarks
    (
        orders_df.write.mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(output_dir / "orders"))
    )
    counts["orders"] = scale_config["orders"]

    items_df = generate_order_items(spark, orders_df, scale_config["products"], scale_config["items_per_order"])
    items_df.coalesce(8).write.mode("overwrite").parquet(str(output_dir / "order_items"))
    counts["order_items"] = scale_config["orders"] * scale_config["items_per_order"]

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce dataset")
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--scale", default="1x", choices=list(SCALES.keys()))
    parser.add_argument("--skewed", action="store_true", help="Generate skewed customer_id distribution")
    args = parser.parse_args()

    print(f"Generating {args.scale} dataset → {args.output_dir}")
    counts = generate(args.output_dir, scale=args.scale, skewed=args.skewed)
    print("\nGenerated row counts:")
    for tbl, n in counts.items():
        print(f"  {tbl:15s}  {n:>12,}")


if __name__ == "__main__":
    main()
