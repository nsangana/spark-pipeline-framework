"""Main pipeline orchestration engine."""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from spark_pipeline.core.config_parser import ConfigParser, PipelineConfig
from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import PipelineExecutionError
from spark_pipeline.core.reader import DataReader
from spark_pipeline.core.writer import DeltaWriter, WriteResult
from spark_pipeline.transformations.registry import TransformationRegistry
from spark_pipeline.validation.validator import ValidationOrchestrator
from spark_pipeline.utils.logging_config import get_logger
from spark_pipeline.utils.spark_utils import SparkSessionFactory

# Import transformation implementations to register them
from spark_pipeline.transformations import sql, python, builtin  # noqa: F401

logger = get_logger(__name__)


@dataclass
class PipelineResult:
    """Result of pipeline execution."""

    status: str
    pipeline_name: str
    metrics: Dict[str, Any]
    validation_passed: bool
    error: Optional[str] = None


class PipelineEngine:
    """Main orchestration engine for pipeline execution.

    Execution flow:
    1. Load and validate configuration
    2. Initialize SparkSession with Delta extensions
    3. Read all data sources
    4. Execute transformation chain sequentially
    5. Run all validation rules
    6. If validations pass: Write to Delta Lake
    7. If validations fail: Raise error, no write
    8. Return PipelineResult with metrics
    """

    def __init__(self, config_path: str):
        """Initialize pipeline engine.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: Optional[PipelineConfig] = None
        self.spark: Optional[SparkSession] = None
        self.context: Optional[PipelineContext] = None

    def execute(self) -> PipelineResult:
        """Execute the complete pipeline.

        Returns:
            PipelineResult with execution metrics

        Raises:
            PipelineExecutionError: If execution fails
        """
        try:
            logger.info("=" * 60)
            logger.info("STARTING PIPELINE EXECUTION")
            logger.info("=" * 60)

            # Step 1: Load configuration
            self._load_config()

            # Step 2: Initialize Spark session
            self._init_spark()

            # Step 3: Read data sources
            self._read_sources()

            # Step 4: Execute transformations
            final_df = self._execute_transformations()

            # Step 5: Run validations
            validation_result = self._run_validations(final_df)

            # Step 6: Conditional write (only if validations pass)
            write_result = self._write_data(final_df, validation_result)

            logger.info("=" * 60)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            return PipelineResult(
                status="success",
                pipeline_name=self.config.name,
                metrics={
                    "rows_written": write_result.rows_written,
                    "validation_rules_executed": len(validation_result.rules_executed),
                    "validation_rules_passed": len(validation_result.rules_passed),
                },
                validation_passed=validation_result.all_passed(),
            )

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return PipelineResult(
                status="failed",
                pipeline_name=self.config.name if self.config else "unknown",
                metrics={},
                validation_passed=False,
                error=str(e),
            )

        finally:
            self._cleanup()

    def _load_config(self) -> None:
        """Load and validate pipeline configuration."""
        logger.info(f"Loading configuration from {self.config_path}")
        parser = ConfigParser()
        self.config = parser.parse(self.config_path)
        logger.info(f"Pipeline: {self.config.name}")

    def _init_spark(self) -> None:
        """Initialize Spark session with configuration."""
        logger.info("Initializing Spark session")

        spark_config = self.config.spark_config
        app_name = spark_config.get("app_name", f"Pipeline-{self.config.name}")
        master = spark_config.get("master", "local[*]")
        config = spark_config.get("config", {})

        self.spark = SparkSessionFactory.create_session(
            app_name=app_name,
            master=master,
            config=config,
            enable_delta=True,
        )

        self.context = PipelineContext(self.spark)
        logger.info("Spark session initialized successfully")

    def _read_sources(self) -> None:
        """Read all configured data sources."""
        logger.info("Reading data sources")
        reader = DataReader(self.spark)
        reader.read_all(self.config.sources, self.context)
        logger.info(f"Loaded {len(self.config.sources)} data sources")

    def _execute_transformations(self):
        """Execute transformation chain.

        Returns:
            Final transformed DataFrame
        """
        transformations = self.config.transformations

        if not transformations:
            logger.info("No transformations configured")
            # Return the first (and likely only) source DataFrame
            source_names = self.context.list_dataframes()
            if not source_names:
                raise PipelineExecutionError("No data sources available")
            return self.context.get_dataframe(source_names[0])

        logger.info(f"Executing {len(transformations)} transformations")

        for i, transform_config in enumerate(transformations, 1):
            logger.info(f"Step {i}/{len(transformations)}: {transform_config['name']}")

            # Create transformation
            transformation = TransformationRegistry.create(transform_config)

            # Execute transformation
            result_df = transformation.execute(self.context)

            # Register result
            self.context.register_dataframe(transformation.output, result_df)

            logger.info(f"Transformation '{transformation.name}' completed")

        # Return the final DataFrame
        final_transform = transformations[-1]
        final_df = self.context.get_dataframe(final_transform["output"])

        return final_df

    def _run_validations(self, df):
        """Run validation rules on the DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult
        """
        logger.info("Running data quality validations")

        validator = ValidationOrchestrator()
        validation_result = validator.validate(df, self.config.validation_config)

        return validation_result

    def _write_data(self, df, validation_result) -> WriteResult:
        """Write data to target (only if validations pass).

        Args:
            df: DataFrame to write
            validation_result: Validation result

        Returns:
            WriteResult

        Raises:
            ValidationError: If validations failed
        """
        logger.info("Writing data to target")

        writer = DeltaWriter()
        write_result = writer.write(df, self.config.target, validation_result)

        logger.info(f"Successfully wrote {write_result.rows_written} rows")

        return write_result

    def _cleanup(self) -> None:
        """Clean up resources."""
        if self.spark:
            logger.info("Stopping Spark session")
            # Note: In some environments, you may want to keep the session alive
            # Comment out the next line if you want to reuse the session
            # self.spark.stop()
