"""Utilities for creating and managing Spark sessions."""

import logging
from typing import Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory for creating configured Spark sessions with Delta Lake support."""

    @staticmethod
    def create_session(
        app_name: str = "SparkPipeline",
        master: str = "local[*]",
        config: Optional[Dict[str, str]] = None,
        enable_delta: bool = True,
    ) -> SparkSession:
        """Create a Spark session with optional Delta Lake extensions.

        Args:
            app_name: Name of the Spark application
            master: Spark master URL (e.g., 'local[*]', 'yarn', 'spark://...')
            config: Additional Spark configuration options
            enable_delta: Whether to enable Delta Lake extensions

        Returns:
            Configured SparkSession instance
        """
        logger.info(f"Creating Spark session: {app_name}")

        builder = SparkSession.builder.appName(app_name).master(master)

        # Add Delta Lake extensions if enabled
        if enable_delta:
            delta_config = {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }
            # Merge with user config, user config takes precedence
            merged_config = {**delta_config, **(config or {})}
        else:
            merged_config = config or {}

        # Apply all configuration options
        for key, value in merged_config.items():
            builder = builder.config(key, value)
            logger.debug(f"Applied config: {key}={value}")

        spark = builder.getOrCreate()

        # Log Spark version and configuration
        logger.info(f"Spark version: {spark.version}")

        # Log master (not available in Spark Connect/Databricks Connect)
        try:
            logger.info(f"Spark master: {spark.sparkContext.master}")
        except Exception:
            # Spark Connect doesn't support sparkContext
            logger.info("Spark master: Databricks Connect (remote)")

        return spark

    @staticmethod
    def stop_session(spark: SparkSession) -> None:
        """Stop a Spark session gracefully.

        Args:
            spark: SparkSession instance to stop
        """
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()


def get_or_create_spark_session(
    app_name: str = "SparkPipeline",
    master: str = "local[*]",
    config: Optional[Dict[str, str]] = None,
    enable_delta: bool = True,
) -> SparkSession:
    """Get existing Spark session or create a new one.

    This is a convenience function that wraps SparkSessionFactory.create_session().

    Args:
        app_name: Name of the Spark application
        master: Spark master URL
        config: Additional Spark configuration options
        enable_delta: Whether to enable Delta Lake extensions

    Returns:
        Configured SparkSession instance
    """
    return SparkSessionFactory.create_session(
        app_name=app_name, master=master, config=config, enable_delta=enable_delta
    )
