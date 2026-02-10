"""Python function-based transformations."""

import importlib
from typing import Callable

from pyspark.sql import DataFrame

from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import TransformationError
from spark_pipeline.transformations.base import Transformation
from spark_pipeline.transformations.registry import register_transformation
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


@register_transformation("python")
class PythonTransformation(Transformation):
    """Execute Python functions as transformations.

    Function should accept DataFrames as positional arguments (one for each input)
    and return a DataFrame.
    """

    def __init__(self, config: dict):
        """Initialize Python transformation.

        Args:
            config: Configuration with 'function' key containing the function path
        """
        super().__init__(config)
        self.function_path = config.get("function")
        self.params = config.get("params", {})

        if not self.function_path:
            raise TransformationError(
                f"Function path not provided for transformation '{self.name}'"
            )

        self.function = self._load_function()

    def _load_function(self) -> Callable:
        """Load the Python function from its module path.

        Returns:
            Function callable

        Raises:
            TransformationError: If function cannot be loaded
        """
        try:
            # Split module path and function name
            # e.g., "transformations.user_analytics.calculate_metrics"
            parts = self.function_path.rsplit(".", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid function path: {self.function_path}")

            module_path, function_name = parts

            # Import module and get function
            module = importlib.import_module(module_path)
            function = getattr(module, function_name)

            logger.debug(f"Loaded function: {self.function_path}")
            return function

        except Exception as e:
            raise TransformationError(
                f"Failed to load function '{self.function_path}': {e}"
            )

    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute Python function.

        Args:
            context: Pipeline context

        Returns:
            Result DataFrame
        """
        logger.info(f"Executing Python transformation: {self.name}")

        try:
            # Get input DataFrames
            input_dfs = self.get_input_dataframes(context)

            # Call function with input DataFrames and params
            if self.params:
                result = self.function(*input_dfs, **self.params)
            else:
                result = self.function(*input_dfs)

            if not isinstance(result, DataFrame):
                raise TransformationError(
                    f"Function '{self.function_path}' must return a DataFrame, "
                    f"got {type(result)}"
                )

            return result

        except TransformationError:
            raise
        except Exception as e:
            raise TransformationError(
                f"Python transformation '{self.name}' failed: {e}"
            )
