"""Spark Declarative Pipeline Framework.

A production-ready framework for building data pipelines with declarative YAML configuration,
data quality validation, and conditional writes to Delta Lake.
"""

__version__ = "0.1.0"

from spark_pipeline.core.engine import PipelineEngine
from spark_pipeline.core.exceptions import (
    PipelineConfigError,
    PipelineExecutionError,
    ValidationError,
)

__all__ = [
    "PipelineEngine",
    "PipelineConfigError",
    "PipelineExecutionError",
    "ValidationError",
]
