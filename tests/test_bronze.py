"""COD-05 unit tests for bronze transformation logic."""

from pyspark.testing.utils import assertDataFrameEqual

from de_excellence_poc.bronze import add_ingestion_metadata


def test_add_ingestion_metadata_adds_columns(spark):
    df = spark.createDataFrame([("APP-001",)], "application_id string")
    result = add_ingestion_metadata(df, "marketplace_india.silver.fact_loan_application")

    # the two metadata columns exist
    assert "_ingested_at" in result.columns
    assert "_source_table" in result.columns
    # source_table value is stamped correctly on every row
    sources = [r["_source_table"] for r in result.collect()]
    assert sources == ["marketplace_india.silver.fact_loan_application"]


def test_add_ingestion_metadata_preserves_original_data(spark):
    # Bronze must be faithful: original columns/values unchanged.
    df = spark.createDataFrame(
        [("APP-001", 1000), ("APP-002", 2000)],
        "application_id string, amount int",
    )
    result = add_ingestion_metadata(df, "src").select("application_id", "amount")
    assertDataFrameEqual(result, df)
