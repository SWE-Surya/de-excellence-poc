"""Pytest configuration and fixtures.

Unit tests (COD-05) use a LOCAL Spark session against in-memory fixtures —
no Databricks workspace, no token, runs in CI. A separate remote session
(Databricks Connect) will be added later for integration/E2E tests (COD-06).
"""

import json
import csv
import pathlib

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local Spark session for unit tests. Deterministic, no remote workspace."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("de-excellence-poc-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Load a JSON or CSV fixture from the fixtures/ directory as a DataFrame."""

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader