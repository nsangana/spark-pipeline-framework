"""Unit tests for configuration parser."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from spark_pipeline.core.config_parser import ConfigParser, PipelineConfig
from spark_pipeline.core.exceptions import PipelineConfigError


def test_parse_valid_config(sample_config, temp_dir):
    """Test parsing a valid configuration."""
    # Write config to file
    config_path = temp_dir / "test_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config, f)

    # Parse config
    parser = ConfigParser()
    config = parser.parse(str(config_path))

    assert isinstance(config, PipelineConfig)
    assert config.name == "test_pipeline"
    assert len(config.sources) == 1
    assert len(config.transformations) == 1


def test_parse_missing_required_field(temp_dir):
    """Test parsing config with missing required field."""
    invalid_config = {
        "pipeline": {"name": "test"},
        # Missing 'sources' field
        "target": {"type": "delta", "path": "/tmp/output"},
    }

    config_path = temp_dir / "invalid_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(invalid_config, f)

    parser = ConfigParser()
    with pytest.raises(PipelineConfigError):
        parser.parse(str(config_path))


def test_env_var_interpolation(temp_dir):
    """Test environment variable interpolation."""
    os.environ["TEST_DATA_PATH"] = "/test/data"
    os.environ["TEST_OUTPUT_PATH"] = "/test/output"

    config = {
        "pipeline": {"name": "test"},
        "sources": [
            {
                "name": "test_source",
                "type": "parquet",
                "path": "${TEST_DATA_PATH}/input",
            }
        ],
        "target": {
            "type": "delta",
            "path": "${TEST_OUTPUT_PATH}/table",
            "mode": "overwrite",
        },
    }

    config_path = temp_dir / "env_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    parser = ConfigParser()
    parsed_config = parser.parse(str(config_path))

    assert parsed_config.sources[0]["path"] == "/test/data/input"
    assert parsed_config.target["path"] == "/test/output/table"

    # Cleanup
    del os.environ["TEST_DATA_PATH"]
    del os.environ["TEST_OUTPUT_PATH"]


def test_env_var_with_default(temp_dir):
    """Test environment variable with default value."""
    config = {
        "pipeline": {"name": "test"},
        "sources": [
            {
                "name": "test_source",
                "type": "parquet",
                "path": "${MISSING_VAR:-/default/path}",
            }
        ],
        "target": {"type": "delta", "path": "/output", "mode": "overwrite"},
    }

    config_path = temp_dir / "default_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    parser = ConfigParser()
    parsed_config = parser.parse(str(config_path))

    assert parsed_config.sources[0]["path"] == "/default/path"


def test_config_properties():
    """Test PipelineConfig properties."""
    config_dict = {
        "pipeline": {"name": "test_pipeline", "description": "Test description"},
        "spark": {"app_name": "TestApp", "master": "local[*]"},
        "sources": [{"name": "src", "type": "parquet", "path": "/data"}],
        "transformations": [
            {
                "name": "transform1",
                "type": "sql",
                "inputs": ["src"],
                "output": "result",
                "sql": "SELECT * FROM src",
            }
        ],
        "validation": {"enabled": True, "fail_on_error": True, "rules": []},
        "target": {"type": "delta", "path": "/output", "mode": "overwrite"},
    }

    config = PipelineConfig(config_dict)

    assert config.name == "test_pipeline"
    assert config.description == "Test description"
    assert config.spark_config["app_name"] == "TestApp"
    assert len(config.sources) == 1
    assert len(config.transformations) == 1
    assert config.validation_config["enabled"] is True
    assert config.target["type"] == "delta"
