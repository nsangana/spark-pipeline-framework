"""Base class for transformations."""

from abc import ABC, abstractmethod
from typing import Dict, List

from pyspark.sql import DataFrame

from spark_pipeline.core.context import PipelineContext


class Transformation(ABC):
    """Abstract base class for all transformations."""

    def __init__(self, config: Dict):
        """Initialize transformation.

        Args:
            config: Transformation configuration dictionary
        """
        self.config = config
        self.name = config["name"]
        self.inputs = config["inputs"]
        self.output = config["output"]

    @abstractmethod
    def execute(self, context: PipelineContext) -> DataFrame:
        """Execute the transformation.

        Args:
            context: Pipeline context containing input DataFrames

        Returns:
            Transformed DataFrame
        """
        pass

    def get_input_dataframes(self, context: PipelineContext) -> List[DataFrame]:
        """Get input DataFrames from context.

        Args:
            context: Pipeline context

        Returns:
            List of input DataFrames
        """
        return [context.get_dataframe(name) for name in self.inputs]

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name={self.name}, inputs={self.inputs}, output={self.output})"
