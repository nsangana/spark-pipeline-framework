"""Integration tests for end-to-end pipeline execution."""

import tempfile
from pathlib import Path

import pytest
import yaml

from spark_pipeline.core.engine import PipelineEngine
from spark_pipeline.core.exceptions import ValidationError


def test_pipeline_execution_with_passing_validation(spark_session, temp_dir):
    """Test complete pipeline execution with passing validation."""
    # Create sample data
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    sample_data = spark_session.createDataFrame(
        [
            (1, "Alice", 25, "2024-01-01"),
            (2, "Bob", 30, "2024-01-01"),
            (3, "Charlie", 35, "2024-01-02"),
        ],
        ["id", "name", "age", "date"],
    )
    sample_data.write.format("parquet").save(str(input_path))

    # Create pipeline config
    config = {
        "pipeline": {"name": "test_pipeline"},
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[*]",
            "config": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
        },
        "sources": [
            {"name": "users", "type": "parquet", "path": str(input_path)}
        ],
        "transformations": [
            {
                "name": "filter_adults",
                "type": "sql",
                "inputs": ["users"],
                "output": "adults",
                "sql": "SELECT * FROM users WHERE age >= 25",
            }
        ],
        "validation": {
            "enabled": True,
            "fail_on_error": True,
            "rules": [
                {
                    "name": "check_nulls",
                    "type": "null_check",
                    "column": "id",
                    "threshold": 0.0,
                },
                {
                    "name": "check_row_count",
                    "type": "row_count",
                    "min_count": 1,
                },
            ],
        },
        "target": {
            "type": "delta",
            "path": str(output_path),
            "mode": "overwrite",
        },
    }

    # Write config file
    config_path = temp_dir / "pipeline.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    # Execute pipeline
    engine = PipelineEngine(str(config_path))
    result = engine.execute()

    # Verify success
    assert result.status == "success"
    assert result.validation_passed is True
    assert result.metrics["rows_written"] == 3

    # Verify output data
    output_df = spark_session.read.format("delta").load(str(output_path))
    assert output_df.count() == 3


def test_pipeline_execution_with_failing_validation(spark_session, temp_dir):
    """Test pipeline execution fails when validation fails (CRITICAL TEST)."""
    # Create sample data with nulls
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    sample_data = spark_session.createDataFrame(
        [
            (1, "Alice", 25, "2024-01-01"),
            (None, "Bob", 30, "2024-01-01"),  # Null ID
            (None, "Charlie", 35, "2024-01-02"),  # Null ID
        ],
        ["id", "name", "age", "date"],
    )
    sample_data.write.format("parquet").save(str(input_path))

    # Create pipeline config with strict null check
    config = {
        "pipeline": {"name": "test_pipeline"},
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[*]",
            "config": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
        },
        "sources": [
            {"name": "users", "type": "parquet", "path": str(input_path)}
        ],
        "validation": {
            "enabled": True,
            "fail_on_error": True,
            "rules": [
                {
                    "name": "check_id_nulls",
                    "type": "null_check",
                    "column": "id",
                    "threshold": 0.0,  # Strict: no nulls allowed
                }
            ],
        },
        "target": {
            "type": "delta",
            "path": str(output_path),
            "mode": "overwrite",
        },
    }

    # Write config file
    config_path = temp_dir / "pipeline.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    # Execute pipeline - should fail
    engine = PipelineEngine(str(config_path))
    result = engine.execute()

    # Verify failure
    assert result.status == "failed"
    assert result.validation_passed is False
    assert "validation failed" in result.error.lower()

    # Verify NO data was written
    try:
        spark_session.read.format("delta").load(str(output_path))
        assert False, "Data should not have been written when validation fails"
    except Exception:
        # Expected - table should not exist
        pass


def test_pipeline_with_multiple_transformations(spark_session, temp_dir):
    """Test pipeline with multiple chained transformations."""
    # Create sample data
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    sample_data = spark_session.createDataFrame(
        [
            (1, "Alice", 25, 50000, "2024-01-01"),
            (2, "Bob", 30, 60000, "2024-01-01"),
            (3, "Charlie", 35, 70000, "2024-01-02"),
            (4, "David", 28, 55000, "2024-01-02"),
        ],
        ["id", "name", "age", "salary", "date"],
    )
    sample_data.write.format("parquet").save(str(input_path))

    # Create pipeline config
    config = {
        "pipeline": {"name": "multi_transform_pipeline"},
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[*]",
        },
        "sources": [
            {"name": "employees", "type": "parquet", "path": str(input_path)}
        ],
        "transformations": [
            {
                "name": "filter_age",
                "type": "filter",
                "inputs": ["employees"],
                "output": "filtered",
                "condition": "age >= 30",
            },
            {
                "name": "select_columns",
                "type": "sql",
                "inputs": ["filtered"],
                "output": "final",
                "sql": "SELECT id, name, salary FROM filtered",
            },
        ],
        "validation": {
            "enabled": True,
            "fail_on_error": True,
            "rules": [
                {
                    "name": "check_count",
                    "type": "row_count",
                    "min_count": 1,
                    "max_count": 10,
                }
            ],
        },
        "target": {
            "type": "delta",
            "path": str(output_path),
            "mode": "overwrite",
        },
    }

    # Write config file
    config_path = temp_dir / "pipeline.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    # Execute pipeline
    engine = PipelineEngine(str(config_path))
    result = engine.execute()

    # Verify success
    assert result.status == "success"
    assert result.validation_passed is True

    # Verify output data - should have 2 rows (Bob and Charlie)
    output_df = spark_session.read.format("delta").load(str(output_path))
    assert output_df.count() == 2
    assert "age" not in output_df.columns  # Age was not selected
    assert "salary" in output_df.columns


def test_pipeline_with_no_transformations(spark_session, temp_dir):
    """Test pipeline with no transformations (direct source to target)."""
    # Create sample data
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    sample_data = spark_session.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["id", "name"],
    )
    sample_data.write.format("parquet").save(str(input_path))

    # Create pipeline config without transformations
    config = {
        "pipeline": {"name": "no_transform_pipeline"},
        "sources": [
            {"name": "data", "type": "parquet", "path": str(input_path)}
        ],
        "validation": {"enabled": False},
        "target": {
            "type": "delta",
            "path": str(output_path),
            "mode": "overwrite",
        },
    }

    # Write config file
    config_path = temp_dir / "pipeline.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    # Execute pipeline
    engine = PipelineEngine(str(config_path))
    result = engine.execute()

    # Verify success
    assert result.status == "success"

    # Verify output data
    output_df = spark_session.read.format("delta").load(str(output_path))
    assert output_df.count() == 2
