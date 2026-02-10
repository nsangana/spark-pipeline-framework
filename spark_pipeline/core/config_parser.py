"""Configuration parser for pipeline YAML files."""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from jsonschema import validate, ValidationError as JsonSchemaValidationError

from spark_pipeline.core.exceptions import PipelineConfigError
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class PipelineConfig:
    """Represents a parsed and validated pipeline configuration."""

    def __init__(self, config_dict: Dict[str, Any]):
        """Initialize pipeline configuration.

        Args:
            config_dict: Dictionary containing the pipeline configuration
        """
        self._config = config_dict
        self.name = config_dict["pipeline"]["name"]
        self.description = config_dict["pipeline"].get("description", "")

    @property
    def spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration."""
        return self._config.get("spark", {})

    @property
    def sources(self) -> List[Dict[str, Any]]:
        """Get data source configurations."""
        return self._config.get("sources", [])

    @property
    def transformations(self) -> List[Dict[str, Any]]:
        """Get transformation configurations."""
        return self._config.get("transformations", [])

    @property
    def validation_config(self) -> Dict[str, Any]:
        """Get validation configuration."""
        return self._config.get("validation", {"enabled": False, "fail_on_error": True})

    @property
    def target(self) -> Dict[str, Any]:
        """Get target configuration."""
        return self._config["target"]

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        return self._config.get(key, default)


class ConfigParser:
    """Parser for pipeline YAML configuration files."""

    def __init__(self, schema_path: Optional[str] = None):
        """Initialize configuration parser.

        Args:
            schema_path: Optional path to JSON schema file for validation
        """
        self.schema_path = schema_path or self._get_default_schema_path()
        self.schema = self._load_schema()

    def _get_default_schema_path(self) -> str:
        """Get the default schema path."""
        # Assume schema is in configs/schemas/pipeline_schema.json
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        schema_path = project_root / "configs" / "schemas" / "pipeline_schema.json"
        return str(schema_path)

    def _load_schema(self) -> Dict[str, Any]:
        """Load JSON schema for configuration validation.

        Returns:
            JSON schema dictionary

        Raises:
            PipelineConfigError: If schema file cannot be loaded
        """
        try:
            with open(self.schema_path, "r") as f:
                schema = json.load(f)
            logger.debug(f"Loaded schema from {self.schema_path}")
            return schema
        except Exception as e:
            raise PipelineConfigError(f"Failed to load schema from {self.schema_path}: {e}")

    def _interpolate_env_vars(self, config: Any) -> Any:
        """Recursively interpolate environment variables in configuration.

        Supports ${VAR_NAME} and ${VAR_NAME:-default_value} syntax.

        Args:
            config: Configuration value (can be dict, list, str, or primitive)

        Returns:
            Configuration with interpolated values
        """
        if isinstance(config, dict):
            return {k: self._interpolate_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._interpolate_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._interpolate_string(config)
        else:
            return config

    def _interpolate_string(self, value: str) -> str:
        """Interpolate environment variables in a string.

        Args:
            value: String potentially containing ${VAR} patterns

        Returns:
            String with environment variables replaced
        """
        # Pattern matches ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r"\$\{([^}:]+)(?::(-[^}]+))?\}"

        def replace(match):
            var_name = match.group(1)
            default_value = match.group(2)

            if default_value and default_value.startswith("-"):
                default_value = default_value[1:]  # Remove leading '-'

            env_value = os.environ.get(var_name)

            if env_value is None:
                if default_value is not None:
                    return default_value
                else:
                    logger.warning(f"Environment variable {var_name} not found, keeping as-is")
                    return match.group(0)

            return env_value

        return re.sub(pattern, replace, value)

    def parse(self, config_path: str) -> PipelineConfig:
        """Parse and validate a pipeline configuration file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            Parsed PipelineConfig object

        Raises:
            PipelineConfigError: If configuration is invalid
        """
        logger.info(f"Parsing configuration from {config_path}")

        # Load YAML file
        try:
            with open(config_path, "r") as f:
                config_dict = yaml.safe_load(f)
        except Exception as e:
            raise PipelineConfigError(f"Failed to load YAML from {config_path}: {e}")

        # Interpolate environment variables
        config_dict = self._interpolate_env_vars(config_dict)

        # Validate against schema
        try:
            validate(instance=config_dict, schema=self.schema)
            logger.info("Configuration validated successfully")
        except JsonSchemaValidationError as e:
            raise PipelineConfigError(f"Configuration validation failed: {e.message}")

        return PipelineConfig(config_dict)

    def validate_config_dict(self, config_dict: Dict[str, Any]) -> bool:
        """Validate a configuration dictionary against the schema.

        Args:
            config_dict: Configuration dictionary

        Returns:
            True if valid

        Raises:
            PipelineConfigError: If configuration is invalid
        """
        try:
            validate(instance=config_dict, schema=self.schema)
            return True
        except JsonSchemaValidationError as e:
            raise PipelineConfigError(f"Configuration validation failed: {e.message}")
