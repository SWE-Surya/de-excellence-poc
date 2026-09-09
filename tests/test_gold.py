"""COD-05 unit tests for gold aggregation logic.

Key behaviours proven against fixtures:
  - grouping by product_name
  - approval_rate = approved / total
  - avg_approved_amount averages ONLY approved applications (ignores rejected zeros)
  - avg_verified_income averages the joined income
"""

from pyspark.testing.utils import assertDataFrameEqual  # noqa: F401 (kept for parity)

from de_excellence_poc.gold import build_gold_df

SILVER_SCHEMA = (
    "product_name string, final_approval_status boolean, "
    "approved_loan_amount double, verified_income double"
)


def test_gold_groups_by_product(spark):
    silver = spark.createDataFrame(
        [
            ("Personal Loan", True, 5000.0, 60000.0),
            ("Personal Loan", False, 0.0, 50000.0),
            ("Auto Loan", True, 8000.0, 70000.0),
        ],
        SILVER_SCHEMA,
    )
    result = build_gold_df(spark, silver)
    assert result.count() == 2
    products = {r["product_name"] for r in result.collect()}
    assert products == {"Personal Loan", "Auto Loan"}


def test_approval_rate_and_approved_only_average(spark):
    silver = spark.createDataFrame(
        [
            ("Personal Loan", True, 5000.0, 60000.0),
            ("Personal Loan", False, 0.0, 40000.0),
        ],
        SILVER_SCHEMA,
    )
    row = build_gold_df(spark, silver).collect()[0]
    assert row["application_count"] == 2
    assert row["approved_count"] == 1
    assert row["approval_rate"] == 0.5
    assert row["avg_approved_amount"] == 5000.0  # approved only, not (5000+0)/2
    assert row["avg_verified_income"] == 50000.0


def test_all_rejected_product_has_null_avg_approved(spark):
    silver = spark.createDataFrame(
        [
            ("Personal Loan", False, 0.0, 30000.0),
            ("Personal Loan", False, 0.0, 35000.0),
        ],
        SILVER_SCHEMA,
    )
    row = build_gold_df(spark, silver).collect()[0]
    assert row["approval_rate"] == 0.0
    assert row["avg_approved_amount"] is None