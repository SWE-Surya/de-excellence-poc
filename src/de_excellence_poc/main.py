"""Pipeline entry point: runs the full medallion pipeline bronze -> silver -> gold.

Reads destination catalog/schema from job parameters (supplied by the bundle
variables per target). Source is the fixed upstream domain marketplace_india.silver.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from de_excellence_poc.bronze import ingest_to_bronze
from de_excellence_poc.gold import build_gold
from de_excellence_poc.silver import build_silver

# Source system (fixed, read-only).
SOURCE_CATALOG = "marketplace_india"
SOURCE_SCHEMA = "silver"

# Source tables -> bronze table names.
BRONZE_TABLES = [
    ("fact_loan_application", "bronze_fact_loan_application"),
    ("dim_income", "bronze_dim_income"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True, help="Destination catalog")
    parser.add_argument("--schema", required=True, help="Destination schema")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    dest = f"{args.catalog}.{args.schema}"

    # --- Bronze: faithful copy of each source table ---
    for source_name, bronze_name in BRONZE_TABLES:
        source_table = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{source_name}"
        target_table = f"{dest}.{bronze_name}"
        print(f"[bronze] {source_table} -> {target_table}")
        ingest_to_bronze(spark, source_table, target_table)

    # --- Silver: clean + aggregate income + join ---
    silver_table = f"{dest}.silver_loan_application"
    print(f"[silver] -> {silver_table}")
    build_silver(
        spark,
        bronze_fact_table=f"{dest}.bronze_fact_loan_application",
        bronze_income_table=f"{dest}.bronze_dim_income",
        target_table=silver_table,
    )

    # --- Gold: per-product metrics ---
    gold_table = f"{dest}.gold_product_metrics"
    print(f"[gold] -> {gold_table}")
    build_gold(spark, silver_table=silver_table, target_table=gold_table)

    print("Pipeline complete.")


if __name__ == "__main__":
    main()
