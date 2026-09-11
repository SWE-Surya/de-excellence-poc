"""COD-06 end-to-end test.

Runs the full transformation chain (silver -> gold) on representative fixture
data and asserts the final gold output. This proves the layers compose correctly
end to end, catching integration issues a unit test on a single function cannot.

Self-contained: fixtures in, final DataFrame out, local Spark, no live catalog.
"""

from de_excellence_poc.silver import build_silver_df
from de_excellence_poc.gold import build_gold_df


def test_pipeline_end_to_end(spark):
    # --- Representative raw-shaped inputs (as bronze would hold them) ---
    # fact: 3 applications across 2 products; one approved, one rejected (0 amount),
    # one approved with no income row.
    fact = spark.createDataFrame(
        [
            # application_id, product_name, final_approval_status, approved_loan_amount
            ("APP-1", "Personal Loan", True, 5000.0),
            ("APP-2", "Personal Loan", False, 0.0),
            ("APP-3", "Auto Loan", True, 8000.0),
        ],
        "application_id string, product_name string, "
        "final_approval_status boolean, approved_loan_amount double",
    )
    # income: APP-1 fans out to 2 rows; APP-2 has 1; APP-3 has NONE (missing).
    income = spark.createDataFrame(
        [
            ("APP-1", 100.0, 1000.0, 10.0, 900.0, 950.0),
            ("APP-1", 50.0, 500.0, 5.0, 400.0, 450.0),
            ("APP-2", 200.0, 2000.0, 20.0, 1800.0, 1900.0),
        ],
        "application_id string, salary double, annual_income double, "
        "other_income double, stated_income double, verified_income double",
    )

    # --- Run the full chain: silver then gold ---
    silver = build_silver_df(fact, income)
    gold = build_gold_df(spark, silver)

    # --- Assert on the final gold output ---
    rows = {r["product_name"]: r for r in gold.collect()}

    # Two products in the output
    assert set(rows) == {"Personal Loan", "Auto Loan"}

    # Personal Loan: 2 applications, 1 approved -> approval_rate 0.5
    pl = rows["Personal Loan"]
    assert pl["application_count"] == 2
    assert pl["approved_count"] == 1
    assert pl["approval_rate"] == 0.5
    # avg approved amount = 5000 (approved only; rejected 0 ignored)
    assert pl["avg_approved_amount"] == 5000.0
    # APP-1 income summed across its 2 rows -> verified_income 1400;
    # APP-2 -> 1900; average over the two = 1650
    assert pl["avg_verified_income"] == 1650.0

    # Auto Loan: 1 application, approved
    al = rows["Auto Loan"]
    assert al["application_count"] == 1
    assert al["approval_rate"] == 1.0
    assert al["avg_approved_amount"] == 8000.0
    # APP-3 had no income row -> LEFT join gave null -> AVG over null is null
    assert al["avg_verified_income"] is None