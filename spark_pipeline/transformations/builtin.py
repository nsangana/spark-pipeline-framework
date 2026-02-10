"""Built-in transformations for common operations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import TransformationError
from spark_pipeline.transformations.base import Transformation
from spark_pipeline.transformations.registry import register_transformation
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


@register_transformation("filter")
class FilterTransformation(Transformation):
    """Filter rows based on a condition."""

    def __init__(self, config: dict):
        """Initialize filter transformation.

        Args:
            config: Configuration with 'condition' key
        """
        super().__init__(config)
        self.condition = config.get("condition")

        if not self.condition:
            raise TransformationError(
                f"Condition not provided for filter transformation '{self.name}'"
            )

    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute filter.

        Args:
            context: Pipeline context

        Returns:
            Filtered DataFrame
        """
        logger.info(f"Executing filter transformation: {self.name}")
        input_dfs = self.get_input_dataframes(context)

        if len(input_dfs) != 1:
            raise TransformationError(
                f"Filter transformation requires exactly 1 input, got {len(input_dfs)}"
            )

        return input_dfs[0].filter(self.condition)


@register_transformation("join")
class JoinTransformation(Transformation):
    """Join two DataFrames."""

    def __init__(self, config: dict):
        """Initialize join transformation.

        Args:
            config: Configuration with 'on', 'how' keys
        """
        super().__init__(config)
        self.on = config.get("on")
        self.how = config.get("how", "inner")

        if not self.on:
            raise TransformationError(
                f"Join column(s) not provided for transformation '{self.name}'"
            )

    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute join.

        Args:
            context: Pipeline context

        Returns:
            Joined DataFrame
        """
        logger.info(f"Executing join transformation: {self.name}")
        input_dfs = self.get_input_dataframes(context)

        if len(input_dfs) != 2:
            raise TransformationError(
                f"Join transformation requires exactly 2 inputs, got {len(input_dfs)}"
            )

        left_df, right_df = input_dfs
        return left_df.join(right_df, on=self.on, how=self.how)


@register_transformation("aggregate")
class AggregateTransformation(Transformation):
    """Aggregate data with grouping and aggregations."""

    def __init__(self, config: dict):
        """Initialize aggregate transformation.

        Args:
            config: Configuration with 'group_by' and 'aggregations' keys
        """
        super().__init__(config)
        self.group_by = config.get("group_by", [])
        self.aggregations = config.get("aggregations", {})

        if not self.aggregations:
            raise TransformationError(
                f"Aggregations not provided for transformation '{self.name}'"
            )

    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute aggregation.

        Args:
            context: Pipeline context

        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Executing aggregate transformation: {self.name}")
        input_dfs = self.get_input_dataframes(context)

        if len(input_dfs) != 1:
            raise TransformationError(
                f"Aggregate transformation requires exactly 1 input, got {len(input_dfs)}"
            )

        df = input_dfs[0]

        # Build aggregation expressions
        agg_exprs = []
        for col_name, agg_func in self.aggregations.items():
            if agg_func == "sum":
                agg_exprs.append(F.sum(col_name).alias(f"{col_name}_sum"))
            elif agg_func == "avg":
                agg_exprs.append(F.avg(col_name).alias(f"{col_name}_avg"))
            elif agg_func == "count":
                agg_exprs.append(F.count(col_name).alias(f"{col_name}_count"))
            elif agg_func == "min":
                agg_exprs.append(F.min(col_name).alias(f"{col_name}_min"))
            elif agg_func == "max":
                agg_exprs.append(F.max(col_name).alias(f"{col_name}_max"))
            else:
                raise TransformationError(f"Unsupported aggregation function: {agg_func}")

        # Group and aggregate
        if self.group_by:
            return df.groupBy(*self.group_by).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)
