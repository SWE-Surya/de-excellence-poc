"""COD-06 unit tests for the schema-contract pure logic.

Tests the pure function against fixtures; no live catalog needed.
"""

from de_excellence_poc.schema_contract import check_missing_columns


def test_no_missing_when_all_present():
    expected = {"application_id", "product_name"}
    actual = {"application_id", "product_name", "extra_col"}
    assert check_missing_columns(expected, actual) == set()


def test_detects_a_missing_column():
    expected = {"application_id", "product_name", "approved_loan_amount"}
    actual = {"application_id", "product_name"}  # approved_loan_amount renamed/dropped
    assert check_missing_columns(expected, actual) == {"approved_loan_amount"}


def test_detects_multiple_missing_columns():
    expected = {"a", "b", "c"}
    actual = {"a"}
    assert check_missing_columns(expected, actual) == {"b", "c"}


def test_empty_actual_means_all_missing():
    expected = {"a", "b"}
    actual = set()
    assert check_missing_columns(expected, actual) == {"a", "b"}
