"""Pipeline entry point. Bronze stage: ingest source tables into our catalog."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from de_excellence_poc.bronze import ingest_to_bronze

# Source is fixed (marketplace_india.silver); destination varies per environment.
SOURCE_CATALOG = "marketplace_india"
SOURCE_SCHEMA = "silver"

# (source_table_name, bronze_target_table_name)
TABLES = [
    ("fact_loan_application", "bronze_fact_loan_application"),
    ("dim_income", "bronze_dim_income"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True, help="Destination catalog")
    parser.add_argument("--schema", required=True, help="Destination schema")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    for source_name, bronze_name in TABLES:
        source_table = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{source_name}"
        target_table = f"{args.catalog}.{args.schema}.{bronze_name}"
        print(f"Bronze: {source_table} -> {target_table}")
        ingest_to_bronze(spark, source_table, target_table)


if __name__ == "__main__":
    main()
