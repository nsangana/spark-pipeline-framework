"""Data readers for various formats."""

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import DataSourceError
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class DataReader:
    """Reader for multiple data formats (Parquet, Delta, CSV, JSON)."""

    def __init__(self, spark: SparkSession):
        """Initialize data reader.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark

    def read(self, source_config: Dict) -> DataFrame:
        """Read data from a configured source.

        Args:
            source_config: Source configuration dictionary with keys:
                - name: Source name
                - type: Source type (parquet, delta, csv, json)
                - path: Source path
                - options: Optional read options

        Returns:
            DataFrame containing the source data

        Raises:
            DataSourceError: If reading fails
        """
        name = source_config["name"]
        source_type = source_config["type"]
        path = source_config["path"]
        options = source_config.get("options", {})

        logger.info(f"Reading source '{name}' of type '{source_type}' from {path}")

        try:
            if source_type == "parquet":
                df = self._read_parquet(path, options)
            elif source_type == "delta":
                df = self._read_delta(path, options)
            elif source_type == "csv":
                df = self._read_csv(path, options)
            elif source_type == "json":
                df = self._read_json(path, options)
            else:
                raise DataSourceError(f"Unsupported source type: {source_type}")

            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from '{name}'")

            return df

        except Exception as e:
            raise DataSourceError(f"Failed to read source '{name}' from {path}: {e}")

    def _read_parquet(self, path: str, options: Dict) -> DataFrame:
        """Read Parquet data.

        Args:
            path: Path to Parquet files
            options: Read options

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("parquet")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(path)

    def _read_delta(self, path: str, options: Dict) -> DataFrame:
        """Read Delta Lake data.

        Args:
            path: Path to Delta table
            options: Read options (e.g., versionAsOf, timestampAsOf)

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("delta")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(path)

    def _read_csv(self, path: str, options: Dict) -> DataFrame:
        """Read CSV data.

        Args:
            path: Path to CSV files
            options: Read options (e.g., header, inferSchema)

        Returns:
            DataFrame
        """
        # Default options for CSV
        default_options = {"header": "true", "inferSchema": "true"}
        merged_options = {**default_options, **options}

        reader = self.spark.read.format("csv")
        for key, value in merged_options.items():
            reader = reader.option(key, value)
        return reader.load(path)

    def _read_json(self, path: str, options: Dict) -> DataFrame:
        """Read JSON data.

        Args:
            path: Path to JSON files
            options: Read options

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("json")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(path)

    def read_all(
        self, sources: List[Dict], context: PipelineContext
    ) -> Dict[str, DataFrame]:
        """Read all configured sources and register them in the context.

        Args:
            sources: List of source configurations
            context: Pipeline context to register DataFrames in

        Returns:
            Dictionary mapping source names to DataFrames

        Raises:
            DataSourceError: If reading any source fails
        """
        dataframes = {}

        for source_config in sources:
            name = source_config["name"]
            df = self.read(source_config)
            context.register_dataframe(name, df)
            dataframes[name] = df

        logger.info(f"Successfully loaded {len(dataframes)} data sources")
        return dataframes
