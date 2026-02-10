"""Unit tests for Delta Lake writer with conditional logic."""

import pytest

from spark_pipeline.core.exceptions import ValidationError
from spark_pipeline.core.writer import DeltaWriter
from spark_pipeline.validation.rules import ValidationResult


def test_write_with_validation_pass(spark_session, sample_dataframe, temp_delta_path):
    """Test write succeeds when validation passes."""
    # Create validation result that passes
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")
    validation_result.add_passed("rule2")

    assert validation_result.all_passed() is True

    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()
    result = writer.write(sample_dataframe, target_config, validation_result)

    assert result.success is True
    assert result.rows_written == 5

    # Verify data was written
    written_df = spark_session.read.format("delta").load(temp_delta_path)
    assert written_df.count() == 5


def test_write_with_validation_fail(spark_session, sample_dataframe, temp_delta_path):
    """Test write fails when validation fails (CRITICAL TEST)."""
    # Create validation result that fails
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")
    validation_result.add_failed("rule2", "Null check failed")

    assert validation_result.all_passed() is False

    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()

    # Write should raise ValidationError
    with pytest.raises(ValidationError) as exc_info:
        writer.write(sample_dataframe, target_config, validation_result)

    assert "validation failed" in str(exc_info.value).lower()
    assert "rule2" in str(exc_info.value)

    # Verify NO data was written
    try:
        spark_session.read.format("delta").load(temp_delta_path)
        assert False, "Data should not have been written"
    except Exception:
        # Expected - table should not exist
        pass


def test_write_without_validation(spark_session, sample_dataframe, temp_delta_path):
    """Test write without validation result (allowed for backward compatibility)."""
    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()
    result = writer.write(sample_dataframe, target_config, validation_result=None)

    assert result.success is True
    assert result.rows_written == 5


def test_write_with_partitions(spark_session, sample_dataframe, temp_delta_path):
    """Test write with partition columns."""
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")

    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
        "partition_by": ["date"],
    }

    writer = DeltaWriter()
    result = writer.write(sample_dataframe, target_config, validation_result)

    assert result.success is True

    # Verify partitioned structure
    written_df = spark_session.read.format("delta").load(temp_delta_path)
    assert written_df.count() == 5


def test_write_append_mode(spark_session, sample_dataframe, temp_delta_path):
    """Test write in append mode."""
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")

    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()

    # First write
    writer.write(sample_dataframe, target_config, validation_result)

    # Second write in append mode
    target_config["mode"] = "append"
    writer.write(sample_dataframe, target_config, validation_result)

    # Should have double the rows
    written_df = spark_session.read.format("delta").load(temp_delta_path)
    assert written_df.count() == 10


def test_write_parquet(spark_session, sample_dataframe, temp_parquet_path):
    """Test write to Parquet format."""
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")

    target_config = {
        "type": "parquet",
        "path": temp_parquet_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()
    result = writer.write(sample_dataframe, target_config, validation_result)

    assert result.success is True

    # Verify data was written
    written_df = spark_session.read.format("parquet").load(temp_parquet_path)
    assert written_df.count() == 5


def test_validation_result_methods():
    """Test ValidationResult helper methods."""
    result = ValidationResult()

    # Initially all pass
    assert result.all_passed() is True
    assert result.get_failures() == []

    # Add some rules
    result.add_passed("rule1")
    result.add_passed("rule2")

    assert result.all_passed() is True
    assert len(result.rules_passed) == 2

    # Add a failure
    result.add_failed("rule3", "Some error")

    assert result.all_passed() is False
    assert result.get_failures() == ["rule3"]
    assert result.failure_details["rule3"] == "Some error"
