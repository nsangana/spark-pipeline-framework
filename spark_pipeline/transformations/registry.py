"""Registry for transformation types."""

from typing import Dict, Type

from spark_pipeline.core.exceptions import TransformationError
from spark_pipeline.transformations.base import Transformation
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class TransformationRegistry:
    """Registry for transformation types.

    Provides a plugin architecture for registering and creating transformations.
    """

    _transformations: Dict[str, Type[Transformation]] = {}

    @classmethod
    def register(cls, transformation_type: str, transformation_class: Type[Transformation]) -> None:
        """Register a transformation type.

        Args:
            transformation_type: Type identifier (e.g., 'sql', 'python')
            transformation_class: Transformation class
        """
        cls._transformations[transformation_type] = transformation_class
        logger.debug(f"Registered transformation type: {transformation_type}")

    @classmethod
    def create(cls, config: Dict) -> Transformation:
        """Create a transformation instance from configuration.

        Args:
            config: Transformation configuration

        Returns:
            Transformation instance

        Raises:
            TransformationError: If transformation type not registered
        """
        transformation_type = config.get("type")

        if transformation_type not in cls._transformations:
            raise TransformationError(
                f"Unknown transformation type: {transformation_type}. "
                f"Available types: {list(cls._transformations.keys())}"
            )

        transformation_class = cls._transformations[transformation_type]
        return transformation_class(config)

    @classmethod
    def list_types(cls) -> list[str]:
        """List all registered transformation types.

        Returns:
            List of transformation type names
        """
        return list(cls._transformations.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered transformations (mainly for testing)."""
        cls._transformations.clear()


def register_transformation(transformation_type: str):
    """Decorator for registering transformation classes.

    Args:
        transformation_type: Type identifier

    Returns:
        Decorator function
    """
    def decorator(cls):
        TransformationRegistry.register(transformation_type, cls)
        return cls
    return decorator
