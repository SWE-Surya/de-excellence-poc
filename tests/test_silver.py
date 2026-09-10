"""COD-05 unit tests for silver transformation logic.

Covers the real edge cases the source data exhibits:
  - income fan-out (multiple income rows per application -> summed)
  - applications with no income row (LEFT join -> nulls preserved)
  - duplicate fact keys (dedupe)
All assertions use assertDataFrameEqual against small deterministic fixtures.
"""

from pyspark.testing.utils import assertDataFrameEqual

from de_excellence_poc.silver import (
    aggregate_income,
    build_silver_df,
    dedupe_by_key,
    join_fact_income,
)

INCOME_SCHEMA = (
    "application_id string, salary double, annual_income double, "
    "other_income double, stated_income double, verified_income double"
)


def test_aggregate_income_sums_fanned_out_rows(spark):
    # APP-1 has two income rows -> summed; APP-2 has one -> unchanged.
    income = spark.createDataFrame(
        [
            ("APP-1", 100.0, 1000.0, 10.0, 900.0, 950.0),
            ("APP-1", 50.0, 500.0, 5.0, 400.0, 450.0),
            ("APP-2", 200.0, 2000.0, 20.0, 1800.0, 1900.0),
        ],
        INCOME_SCHEMA,
    )
    result = aggregate_income(income)
    expected = spark.createDataFrame(
        [
            ("APP-1", 150.0, 1500.0, 15.0, 1300.0, 1400.0),
            ("APP-2", 200.0, 2000.0, 20.0, 1800.0, 1900.0),
        ],
        INCOME_SCHEMA,
    )
    assertDataFrameEqual(result, expected)


def test_aggregate_income_one_row_per_application(spark):
    income = spark.createDataFrame(
        [
            ("APP-1", 100.0, 1000.0, 10.0, 900.0, 950.0),
            ("APP-1", 50.0, 500.0, 5.0, 400.0, 450.0),
        ],
        INCOME_SCHEMA,
    )
    result = aggregate_income(income)
    assert result.count() == 1


def test_left_join_keeps_application_with_no_income(spark):
    # APP-2 has no income row -> kept, income columns null.
    fact = spark.createDataFrame(
        [("APP-1", 5000.0), ("APP-2", 3000.0)],
        "application_id string, approved_loan_amount double",
    )
    agg_income = spark.createDataFrame(
        [("APP-1", 150.0)],
        "application_id string, verified_income double",
    )
    result = join_fact_income(fact, agg_income)

    # both applications survive
    assert result.count() == 2
    # APP-2's income is null
    app2 = result.filter(result.application_id == "APP-2").collect()[0]
    assert app2["verified_income"] is None


def test_dedupe_removes_duplicate_application(spark):
    fact = spark.createDataFrame(
        [("APP-1", 5000.0), ("APP-1", 5000.0), ("APP-2", 3000.0)],
        "application_id string, approved_loan_amount double",
    )
    result = dedupe_by_key(fact, ["application_id"])
    assert result.count() == 2


def test_dedupe_empty_keys_raises(spark):
    fact = spark.createDataFrame([("APP-1",)], "application_id string")
    try:
        dedupe_by_key(fact, [])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_build_silver_end_to_end_no_fanout(spark):
    # Full transform: 2 applications, APP-1 income fans out, APP-2 has no income.
    fact = spark.createDataFrame(
        [("APP-1", 5000.0), ("APP-2", 3000.0)],
        "application_id string, approved_loan_amount double",
    )
    income = spark.createDataFrame(
        [
            ("APP-1", 100.0, 1000.0, 10.0, 900.0, 950.0),
            ("APP-1", 50.0, 500.0, 5.0, 400.0, 450.0),
        ],
        INCOME_SCHEMA,
    )
    result = build_silver_df(fact, income)

    assert result.count() == 2  # no fan-out
    app1 = result.filter(result.application_id == "APP-1").collect()[0]
    assert app1["verified_income"] == 1400.0  # summed
    app2 = result.filter(result.application_id == "APP-2").collect()[0]
    assert app2["verified_income"] is None  # left join
