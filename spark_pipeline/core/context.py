"""Pipeline execution context."""

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from spark_pipeline.core.exceptions import PipelineExecutionError
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class PipelineContext:
    """Context for pipeline execution.

    Holds the Spark session, registered DataFrames, and other execution state.
    """

    def __init__(self, spark: SparkSession):
        """Initialize pipeline context.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self._dataframes: Dict[str, DataFrame] = {}
        self._metadata: Dict[str, any] = {}

    def register_dataframe(self, name: str, df: DataFrame) -> None:
        """Register a DataFrame with a name.

        Args:
            name: Name to register the DataFrame under
            df: DataFrame to register
        """
        logger.debug(f"Registering DataFrame: {name}")
        self._dataframes[name] = df

        # Also register as temp view for SQL access
        df.createOrReplaceTempView(name)

    def get_dataframe(self, name: str) -> DataFrame:
        """Get a registered DataFrame by name.

        Args:
            name: Name of the DataFrame

        Returns:
            DataFrame instance

        Raises:
            PipelineExecutionError: If DataFrame not found
        """
        if name not in self._dataframes:
            raise PipelineExecutionError(f"DataFrame '{name}' not found in context")
        return self._dataframes[name]

    def has_dataframe(self, name: str) -> bool:
        """Check if a DataFrame is registered.

        Args:
            name: Name of the DataFrame

        Returns:
            True if DataFrame exists
        """
        return name in self._dataframes

    def list_dataframes(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of DataFrame names
        """
        return list(self._dataframes.keys())

    def set_metadata(self, key: str, value: any) -> None:
        """Set a metadata value.

        Args:
            key: Metadata key
            value: Metadata value
        """
        self._metadata[key] = value

    def get_metadata(self, key: str, default: any = None) -> any:
        """Get a metadata value.

        Args:
            key: Metadata key
            default: Default value if key not found

        Returns:
            Metadata value
        """
        return self._metadata.get(key, default)

    def clear(self) -> None:
        """Clear all registered DataFrames and metadata."""
        logger.debug("Clearing pipeline context")
        self._dataframes.clear()
        self._metadata.clear()

    def __repr__(self) -> str:
        """String representation of context."""
        return f"PipelineContext(dataframes={list(self._dataframes.keys())})"
