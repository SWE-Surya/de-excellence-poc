"""Silver layer: clean and conform bronze into an analysis-ready table.

Design (COD-05): every transformation is a PURE function (DataFrame in -> DataFrame
out, no I/O), so it is unit-testable against small fixtures. Reads and writes live
only in the thin `build_silver` wrapper, which is not unit-tested.

Business logic, driven by what the source data actually looks like:
  - dim_income fans out (multiple income rows per application) -> aggregate to one
    row per application_id, SUMMING the monetary columns (distinct income sources).
  - Not every application has an income row -> LEFT join keeps all applications;
    missing income becomes NULL.
  - The fact key is clean in current data, but we dedupe defensively and test it.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Monetary columns in dim_income that we sum when collapsing to one row per application.
INCOME_SUM_COLUMNS = [
    "salary",
    "annual_income",
    "other_income",
    "stated_income",
    "verified_income",
]


def dedupe_by_key(df: DataFrame, key_cols: list[str]) -> DataFrame:
    """Remove duplicate rows on a business key, keeping one row per key.

    Defensive: current fact data has no duplicate application_id, but this
    guarantees idempotency and is covered by a fixture test.
    """
    if not key_cols:
        raise ValueError("key_cols must not be empty")
    return df.dropDuplicates(key_cols)


def aggregate_income(income_df: DataFrame) -> DataFrame:
    """Collapse dim_income to one row per application_id by SUMMING monetary columns.

    Prevents fan-out double-counting when income is joined to the fact.
    Only application_id and the summed measures are returned.
    """
    sum_exprs = [F.sum(F.col(c)).alias(c) for c in INCOME_SUM_COLUMNS]
    return income_df.groupBy("application_id").agg(*sum_exprs)


def join_fact_income(fact_df: DataFrame, agg_income_df: DataFrame) -> DataFrame:
    """LEFT join fact to aggregated income on application_id.

    LEFT keeps every application, including those with no income record
    (their income columns will be NULL).
    """
    return fact_df.join(agg_income_df, on="application_id", how="left")


def build_silver_df(fact_df: DataFrame, income_df: DataFrame) -> DataFrame:
    """Full silver transformation, composed from the pure functions above.

    Pure: takes the two bronze DataFrames, returns the silver DataFrame. No I/O.
    """
    fact_clean = dedupe_by_key(fact_df, ["application_id"])
    income_agg = aggregate_income(income_df)
    return join_fact_income(fact_clean, income_agg)


def build_silver(
    spark,
    bronze_fact_table: str,
    bronze_income_table: str,
    target_table: str,
) -> None:
    """Thin I/O wrapper: read bronze, apply build_silver_df, write silver."""
    fact_df = spark.read.table(bronze_fact_table)
    income_df = spark.read.table(bronze_income_table)
    silver_df = build_silver_df(fact_df, income_df)
    (silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table))
