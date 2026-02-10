"""Custom exceptions for the Spark pipeline framework."""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    pass


class PipelineConfigError(PipelineError):
    """Raised when there is an error in pipeline configuration."""

    pass


class PipelineExecutionError(PipelineError):
    """Raised when there is an error during pipeline execution."""

    pass


class ValidationError(PipelineError):
    """Raised when data validation fails.

    This exception is raised when data quality validation rules fail,
    preventing data from being written to the target.
    """

    pass


class TransformationError(PipelineError):
    """Raised when there is an error during transformation execution."""

    pass


class DataSourceError(PipelineError):
    """Raised when there is an error reading from a data source."""

    pass


class DataWriteError(PipelineError):
    """Raised when there is an error writing data to the target."""

    pass
