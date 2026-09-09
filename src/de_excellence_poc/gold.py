"""Gold layer: business-facing metrics per product, aggregated from silver.

Design (COD-05): the aggregation is expressed as SQL (readable/reviewable) but
wrapped in a PURE function (DataFrame in -> DataFrame out) so it stays unit-testable
against fixtures. The SQL runs against a temp view of the input DataFrame, never a
live catalog table, so tests need no workspace.

Business nuance from the data: rejected applications have approved_loan_amount = 0.
So we do NOT average those zeros as if approved. We compute:
  - application_count       : all applications for the product
  - approved_count          : applications with approval
  - approval_rate           : approved_count / application_count
  - avg_approved_amount     : average approved_loan_amount AMONG APPROVED ONLY
  - avg_verified_income     : average verified_income (from the silver join)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

GOLD_METRICS_SQL = """
SELECT
    product_name,
    COUNT(*)                                             AS application_count,
    SUM(CASE WHEN final_approval_status THEN 1 ELSE 0 END) AS approved_count,
    ROUND(
        SUM(CASE WHEN final_approval_status THEN 1 ELSE 0 END) / COUNT(*),
        4
    )                                                    AS approval_rate,
    ROUND(
        AVG(CASE WHEN final_approval_status THEN approved_loan_amount END),
        2
    )                                                    AS avg_approved_amount,
    ROUND(AVG(verified_income), 2)                       AS avg_verified_income
FROM {view}
GROUP BY product_name
"""


def build_gold_df(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """Aggregate silver into per-product metrics. Pure: DataFrame in -> DataFrame out.

    Uses a temporary view so the SQL runs against the passed DataFrame only.
    """
    view_name = "silver_input_for_gold"
    silver_df.createOrReplaceTempView(view_name)
    return spark.sql(GOLD_METRICS_SQL.format(view=view_name))


def build_gold(spark: SparkSession, silver_table: str, target_table: str) -> None:
    """Thin I/O wrapper: read silver, apply build_gold_df, write gold."""
    silver_df = spark.read.table(silver_table)
    gold_df = build_gold_df(spark, silver_df)
    (
        gold_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )