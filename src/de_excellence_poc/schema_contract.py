"""COD-06 schema contract check.

Before deploying, verify that the live source tables still contain the columns
the pipeline depends on. A renamed or dropped column is then caught by a fast,
read-only check rather than by a failed production run.

Design (same I/O-vs-logic split as the rest of the project):
  - check_missing_columns(...) is a PURE function (sets in, set out), unit-tested.
  - assert_source_contract(spark, ...) is a thin wrapper that reads the live
    schema from information_schema.columns and calls the pure function.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# The columns the pipeline actually depends on, per source table.
# Keep this in sync with what bronze/silver/gold read.
EXPECTED_COLUMNS: dict[str, set[str]] = {
    "fact_loan_application": {
        "application_id",
        "product_name",
        "final_approval_status",
        "approved_loan_amount",
    },
    "dim_income": {
        "application_id",
        "salary",
        "annual_income",
        "other_income",
        "stated_income",
        "verified_income",
    },
}


def check_missing_columns(expected: set[str], actual: set[str]) -> set[str]:
    """Return the expected columns that are missing from actual. Pure function.

    Empty set means the contract holds for this table.
    """
    return expected - actual


def read_actual_columns(spark: SparkSession, catalog: str, schema: str, table: str) -> set[str]:
    """Read the live column names for a table from information_schema. Read-only I/O."""
    rows = (
        spark.table(f"{catalog}.information_schema.columns")
        .where(f"table_schema = '{schema}' AND table_name = '{table}'")
        .select("column_name")
        .collect()
    )
    return {r["column_name"] for r in rows}


def assert_source_contract(
    spark: SparkSession,
    catalog: str = "marketplace_india",
    schema: str = "silver",
    expected: dict[str, set[str]] | None = None,
) -> None:
    """Check every expected table/column against the live schema.

    Raises ValueError listing all missing columns if the contract is broken.
    Read-only: only queries information_schema, deploys nothing.
    """
    expected = expected or EXPECTED_COLUMNS
    problems: list[str] = []
    for table, cols in expected.items():
        actual = read_actual_columns(spark, catalog, schema, table)
        if not actual:
            problems.append(f"{catalog}.{schema}.{table}: table not found or no columns")
            continue
        missing = check_missing_columns(cols, actual)
        if missing:
            problems.append(f"{catalog}.{schema}.{table}: missing columns {sorted(missing)}")

    if problems:
        raise ValueError("Schema contract check failed:\n  " + "\n  ".join(problems))
    print("Schema contract check passed: all expected source columns present.")
