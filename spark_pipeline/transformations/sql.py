"""SQL-based transformations."""

from pyspark.sql import DataFrame

from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import TransformationError
from spark_pipeline.transformations.base import Transformation
from spark_pipeline.transformations.registry import register_transformation
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


@register_transformation("sql")
class SQLTransformation(Transformation):
    """Execute SQL queries as transformations.

    Input DataFrames are registered as temp views with their names.
    """

    def __init__(self, config: dict):
        """Initialize SQL transformation.

        Args:
            config: Configuration with 'sql' key containing the query
        """
        super().__init__(config)
        self.sql = config.get("sql")

        if not self.sql:
            raise TransformationError(f"SQL query not provided for transformation '{self.name}'")

    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute SQL query.

        Args:
            context: Pipeline context

        Returns:
            Result DataFrame
        """
        logger.info(f"Executing SQL transformation: {self.name}")
        logger.debug(f"SQL: {self.sql}")

        try:
            # Input DataFrames are already registered as temp views in context
            result = context.spark.sql(self.sql)
            return result
        except Exception as e:
            raise TransformationError(
                f"SQL transformation '{self.name}' failed: {e}"
            )
