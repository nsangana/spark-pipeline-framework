"""Shared pytest fixtures."""

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing.

    Yields:
        SparkSession configured for testing with Delta Lake support
    """
    from spark_pipeline.utils.spark_utils import SparkSessionFactory

    spark = SparkSessionFactory.create_session(
        app_name="PipelineTests",
        master="local[2]",
        config={
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.ui.enabled": "false",
        },
        enable_delta=True,
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing.

    Args:
        spark_session: Spark session fixture

    Returns:
        Sample DataFrame with test data
    """
    data = [
        (1, "Alice", 25, 50000, "2024-01-01"),
        (2, "Bob", 30, 60000, "2024-01-01"),
        (3, "Charlie", 35, 70000, "2024-01-02"),
        (4, "David", 28, 55000, "2024-01-02"),
        (5, "Eve", 32, 65000, "2024-01-03"),
    ]
    columns = ["id", "name", "age", "salary", "date"]

    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests.

    Yields:
        Path to temporary directory
    """
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)

    # Cleanup
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def temp_delta_path(temp_dir):
    """Create a temporary path for Delta Lake tables.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Path for Delta Lake table
    """
    delta_path = temp_dir / "delta_table"
    return str(delta_path)


@pytest.fixture
def temp_parquet_path(temp_dir):
    """Create a temporary path for Parquet files.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Path for Parquet files
    """
    parquet_path = temp_dir / "parquet_data"
    return str(parquet_path)


@pytest.fixture
def sample_config(temp_dir):
    """Create a sample pipeline configuration.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Dictionary with sample configuration
    """
    return {
        "pipeline": {
            "name": "test_pipeline",
            "description": "Test pipeline configuration",
        },
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[*]",
            "config": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
        },
        "sources": [
            {
                "name": "test_source",
                "type": "parquet",
                "path": str(temp_dir / "input"),
            }
        ],
        "transformations": [
            {
                "name": "filter_test",
                "type": "sql",
                "inputs": ["test_source"],
                "output": "filtered_data",
                "sql": "SELECT * FROM test_source WHERE age > 25",
            }
        ],
        "validation": {
            "enabled": True,
            "fail_on_error": True,
            "rules": [
                {
                    "name": "null_check_id",
                    "type": "null_check",
                    "column": "id",
                    "threshold": 0.0,
                }
            ],
        },
        "target": {
            "type": "delta",
            "path": str(temp_dir / "output"),
            "mode": "overwrite",
        },
    }
