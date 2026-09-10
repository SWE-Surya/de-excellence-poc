"""Bronze layer: faithful ingestion of source tables into our catalog.

Design (COD-05): transformation logic is a pure function (add_ingestion_metadata)
with no I/O, so it is unit-testable against fixtures. Reads and writes live in a
thin wrapper (ingest_to_bronze) that is not unit-tested.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, source_table: str) -> DataFrame:
    """Add ingestion metadata to a source DataFrame. Pure: DataFrame in, DataFrame out.

    Adds:
      _ingested_at   - when our pipeline loaded the row (audit/lineage)
      _source_table  - the fully-qualified source it came from (traceability)

    No cleaning or filtering: bronze is a faithful copy of the source.
    """
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_table", F.lit(source_table))


def ingest_to_bronze(
    spark: SparkSession,
    source_table: str,
    target_table: str,
) -> None:
    """Read a source table, add metadata, write to bronze. Thin I/O wrapper.

    source_table: fully-qualified source, e.g. marketplace_india.silver.fact_loan_application
    target_table: fully-qualified target, e.g. de_excellence_poc.surya_dev.bronze_fact_loan_application
    """
    source_df = spark.read.table(source_table)
    bronze_df = add_ingestion_metadata(source_df, source_table)
    (bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table))
