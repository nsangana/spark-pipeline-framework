"""Delta Lake writer with conditional write logic based on validation results."""

from dataclasses import dataclass
from typing import Dict, List, Optional

from pyspark.sql import DataFrame

from spark_pipeline.core.exceptions import DataWriteError, ValidationError
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class WriteResult:
    """Result of a write operation."""

    success: bool
    path: str
    mode: str
    rows_written: int
    metrics: Dict[str, any]


class DeltaWriter:
    """Writer for Delta Lake with conditional logic based on validation.

    CRITICAL: This writer ONLY writes data if ALL validation rules pass.
    If any validation fails, it raises ValidationError and prevents data write.
    """

    def write(
        self,
        df: DataFrame,
        target_config: Dict,
        validation_result: Optional[object] = None,
    ) -> WriteResult:
        """Write DataFrame to Delta Lake, only if validations pass.

        Args:
            df: DataFrame to write
            target_config: Target configuration with keys:
                - type: Target type (delta, parquet)
                - path: Target path
                - mode: Write mode (append, overwrite, merge)
                - partition_by: Optional list of partition columns
                - merge_keys: Required for merge mode
                - optimize: Optional optimization config
                - vacuum: Optional vacuum config
            validation_result: Validation result object (must have all_passed() method)

        Returns:
            WriteResult with metrics

        Raises:
            ValidationError: If validation result indicates failures
            DataWriteError: If write operation fails
        """
        # CRITICAL: Check validation result first
        if validation_result is not None:
            if not validation_result.all_passed():
                failed_rules = validation_result.get_failures()
                error_msg = (
                    f"Data quality validation failed. Will NOT write to table. "
                    f"Failed rules: {', '.join(failed_rules)}"
                )
                logger.error(error_msg)
                raise ValidationError(error_msg)
            else:
                logger.info("All validation rules passed. Proceeding with write.")

        # Extract configuration
        target_type = target_config["type"]
        path = target_config["path"]
        mode = target_config.get("mode", "append")
        partition_by = target_config.get("partition_by", [])

        logger.info(f"Writing data to {target_type} at {path} with mode={mode}")

        try:
            # Get row count before write
            rows_written = df.count()

            # Write based on target type
            if target_type == "delta":
                self._write_delta(df, path, mode, partition_by, target_config)
            elif target_type == "parquet":
                self._write_parquet(df, path, mode, partition_by)
            else:
                raise DataWriteError(f"Unsupported target type: {target_type}")

            logger.info(f"Successfully wrote {rows_written} rows to {path}")

            # Perform post-write operations for Delta
            if target_type == "delta":
                self._post_write_operations(path, target_config)

            return WriteResult(
                success=True,
                path=path,
                mode=mode,
                rows_written=rows_written,
                metrics={"rows_written": rows_written, "target_type": target_type},
            )

        except ValidationError:
            # Re-raise validation errors without wrapping
            raise
        except Exception as e:
            raise DataWriteError(f"Failed to write data to {path}: {e}")

    def _write_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_by: List[str],
        target_config: Dict,
    ) -> None:
        """Write to Delta Lake.

        Args:
            df: DataFrame to write
            path: Target path or table name (catalog.schema.table for Unity Catalog)
            mode: Write mode
            partition_by: Partition columns
            target_config: Target configuration
        """
        if mode == "merge":
            self._write_delta_merge(df, path, target_config)
        else:
            writer = df.write.format("delta").mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            # Check if path is a Unity Catalog table name (format: catalog.schema.table)
            # Unity Catalog tables have exactly 2 dots and no slashes
            is_unity_catalog_table = (
                path.count('.') >= 2 and
                '/' not in path and
                not path.startswith('dbfs:') and
                not path.startswith('s3:') and
                not path.startswith('abfss:')
            )

            if is_unity_catalog_table:
                # Use saveAsTable for Unity Catalog tables
                logger.info(f"Writing to Unity Catalog table: {path}")
                writer.saveAsTable(path)
            else:
                # Use save for file paths
                writer.save(path)

    def _write_delta_merge(self, df: DataFrame, path: str, target_config: Dict) -> None:
        """Perform Delta merge (upsert) operation.

        Args:
            df: Source DataFrame
            path: Target Delta table path or table name (catalog.schema.table)
            target_config: Target configuration with merge_keys
        """
        from delta.tables import DeltaTable

        merge_keys = target_config.get("merge_keys", [])
        if not merge_keys:
            raise DataWriteError("merge_keys required for merge mode")

        # Check if path is a Unity Catalog table name
        is_unity_catalog_table = (
            path.count('.') >= 2 and
            '/' not in path and
            not path.startswith('dbfs:') and
            not path.startswith('s3:') and
            not path.startswith('abfss:')
        )

        spark = df.sparkSession

        # Check if target table exists
        try:
            if is_unity_catalog_table:
                # Use forName for Unity Catalog tables
                target_table = DeltaTable.forName(spark, path)
            else:
                # Use forPath for file paths
                if not DeltaTable.isDeltaTable(spark, path):
                    raise Exception(f"Table does not exist at {path}")
                target_table = DeltaTable.forPath(spark, path)

            # Build merge condition
            merge_condition = " AND ".join(
                [f"target.{key} = source.{key}" for key in merge_keys]
            )

            # Perform merge
            (
                target_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            logger.info(f"Delta merge completed for {path}")

        except Exception as e:
            # Table doesn't exist, write as new table
            logger.info(f"Target table {path} doesn't exist, creating new table: {e}")
            self._write_delta(df, path, "overwrite", target_config.get("partition_by", []), target_config)

    def _write_parquet(
        self, df: DataFrame, path: str, mode: str, partition_by: List[str]
    ) -> None:
        """Write to Parquet.

        Args:
            df: DataFrame to write
            path: Target path
            mode: Write mode
            partition_by: Partition columns
        """
        writer = df.write.format("parquet").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)

    def _post_write_operations(self, path: str, target_config: Dict) -> None:
        """Perform post-write operations like optimize and vacuum.

        Args:
            path: Delta table path or table name (catalog.schema.table)
            target_config: Target configuration
        """
        optimize_config = target_config.get("optimize", {})
        vacuum_config = target_config.get("vacuum", {})

        # Get Spark session
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable

        spark = SparkSession.getActiveSession()
        if not spark:
            logger.warning("No active Spark session, skipping post-write operations")
            return

        # Check if path is a Unity Catalog table name
        is_unity_catalog_table = (
            path.count('.') >= 2 and
            '/' not in path and
            not path.startswith('dbfs:') and
            not path.startswith('s3:') and
            not path.startswith('abfss:')
        )

        if is_unity_catalog_table:
            delta_table = DeltaTable.forName(spark, path)
        else:
            delta_table = DeltaTable.forPath(spark, path)

        # Optimize
        if optimize_config.get("enabled", False):
            logger.info(f"Running OPTIMIZE on {path}")
            zorder_by = optimize_config.get("zorder_by", [])

            if zorder_by:
                delta_table.optimize().executeZOrderBy(*zorder_by)
                logger.info(f"OPTIMIZE with ZORDER BY {zorder_by} completed")
            else:
                delta_table.optimize().executeCompaction()
                logger.info("OPTIMIZE compaction completed")

        # Vacuum
        if vacuum_config.get("enabled", False):
            retention_hours = vacuum_config.get("retention_hours", 168)
            logger.info(f"Running VACUUM on {path} with retention={retention_hours} hours")
            delta_table.vacuum(retention_hours)
            logger.info("VACUUM completed")
