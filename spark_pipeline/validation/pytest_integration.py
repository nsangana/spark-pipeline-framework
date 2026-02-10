"""pytest integration for data quality testing."""

import pytest
from pyspark.sql import DataFrame, SparkSession


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "data_quality: mark test as a data quality validation test"
    )


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing.

    Yields:
        SparkSession configured for testing with Delta Lake support
    """
    from spark_pipeline.utils.spark_utils import SparkSessionFactory

    spark = SparkSessionFactory.create_session(
        app_name="DataQualityTests",
        master="local[*]",
        config={
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        },
        enable_delta=True,
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session: SparkSession) -> DataFrame:
    """Create a sample DataFrame for testing.

    Args:
        spark_session: Spark session fixture

    Returns:
        Sample DataFrame
    """
    data = [
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-01"),
        (3, "Charlie", 35, "2024-01-02"),
    ]
    columns = ["id", "name", "age", "date"]

    return spark_session.createDataFrame(data, columns)


class DataQualityTest:
    """Base class for data quality tests.

    Subclass this to create custom data quality tests that can be
    run as part of the validation pipeline.
    """

    def __init__(self, df: DataFrame):
        """Initialize with DataFrame to test.

        Args:
            df: DataFrame to validate
        """
        self.df = df

    def run(self) -> tuple[bool, str]:
        """Run the test.

        Returns:
            Tuple of (passed: bool, message: str)

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement run()")
