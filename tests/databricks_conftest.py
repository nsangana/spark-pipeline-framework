"""Databricks-specific test fixtures using Databricks Connect."""

import os
import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def databricks_spark_session():
    """Create a Databricks Connect SparkSession.

    Uses the spark-pipeline-testing profile with serverless compute.
    """
    # Set Databricks profile
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "spark-pipeline-testing"

    # Create Databricks Connect session
    spark = DatabricksSession.builder.profile("spark-pipeline-testing").getOrCreate()

    # Verify connection
    print(f"\n✓ Connected to Databricks: {spark.version}")
    print(f"✓ Using serverless compute")

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def test_catalog():
    """Return the catalog name for testing."""
    return "nsangana_catalog"


@pytest.fixture(scope="session")
def test_schema(databricks_spark_session, test_catalog):
    """Create and return a test schema in nsangana_catalog.

    The schema will be named: spark_pipeline_test
    """
    schema_name = "spark_pipeline_test"
    full_schema = f"{test_catalog}.{schema_name}"

    # Create schema if it doesn't exist
    databricks_spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")

    print(f"\n✓ Using schema: {full_schema}")

    yield full_schema

    # Cleanup is optional - keeping test schema for inspection


@pytest.fixture
def test_table_path(test_schema):
    """Generate a unique table path for tests."""
    import uuid

    table_name = f"test_table_{uuid.uuid4().hex[:8]}"
    return f"{test_schema}.{table_name}"


@pytest.fixture
def sample_databricks_df(databricks_spark_session):
    """Create a sample DataFrame for testing on Databricks."""
    data = [
        (1, "Alice", 25, 50000, "2024-01-01"),
        (2, "Bob", 30, 60000, "2024-01-01"),
        (3, "Charlie", 35, 70000, "2024-01-02"),
        (4, "David", 28, 55000, "2024-01-02"),
        (5, "Eve", 32, 65000, "2024-01-03"),
    ]
    columns = ["id", "name", "age", "salary", "date"]

    return databricks_spark_session.createDataFrame(data, columns)
