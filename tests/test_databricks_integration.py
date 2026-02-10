"""Integration tests for Databricks using Databricks Connect.

Run with: pytest tests/test_databricks_integration.py -v

These tests use serverless compute and nsangana_catalog.
"""

import pytest
from tests.databricks_conftest import (
    databricks_spark_session,
    sample_databricks_df,
    test_catalog,
    test_schema,
    test_table_path,
)

from spark_pipeline.core.exceptions import ValidationError
from spark_pipeline.core.writer import DeltaWriter
from spark_pipeline.validation.builtin_rules import (
    NullCheckRule,
    RangeCheckRule,
    RowCountRule,
)
from spark_pipeline.validation.rules import ValidationResult
from spark_pipeline.validation.validator import ValidationOrchestrator


def test_databricks_connection(databricks_spark_session, test_schema):
    """Test basic Databricks connection."""
    # Test simple query
    df = databricks_spark_session.sql("SELECT 1 as test")
    result = df.collect()[0]["test"]

    assert result == 1
    print(f"\n✓ Databricks connection successful")
    print(f"✓ Connected to schema: {test_schema}")


def test_create_sample_data(databricks_spark_session, sample_databricks_df, test_schema):
    """Test creating sample data in Databricks."""
    # Count rows
    count = sample_databricks_df.count()
    assert count == 5

    # Test schema
    assert "id" in sample_databricks_df.columns
    assert "name" in sample_databricks_df.columns

    print(f"\n✓ Created sample DataFrame with {count} rows")


def test_validation_rules_on_databricks(databricks_spark_session, sample_databricks_df):
    """Test validation rules execute correctly on Databricks."""
    # Test null check
    null_rule = NullCheckRule(
        {"name": "check_id", "type": "null_check", "column": "id", "threshold": 0.0}
    )
    passed, message = null_rule.validate(sample_databricks_df)
    assert passed is True
    print(f"\n✓ Null check passed: {message}")

    # Test range check
    range_rule = RangeCheckRule(
        {
            "name": "check_age",
            "type": "range_check",
            "column": "age",
            "min_value": 20,
            "max_value": 40,
        }
    )
    passed, message = range_rule.validate(sample_databricks_df)
    assert passed is True
    print(f"✓ Range check passed: {message}")

    # Test row count
    row_count_rule = RowCountRule(
        {"name": "check_count", "type": "row_count", "min_count": 1, "max_count": 100}
    )
    passed, message = row_count_rule.validate(sample_databricks_df)
    assert passed is True
    print(f"✓ Row count check passed: {message}")


def test_write_to_delta_with_validation_pass(
    databricks_spark_session, sample_databricks_df, test_table_path
):
    """Test writing to Delta Lake when validation passes."""
    # Create passing validation result
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")
    validation_result.add_passed("rule2")

    target_config = {
        "type": "delta",
        "path": test_table_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()
    result = writer.write(sample_databricks_df, target_config, validation_result)

    assert result.success is True
    assert result.rows_written == 5

    print(f"\n✓ Successfully wrote data to: {test_table_path}")

    # Verify data was written
    written_df = databricks_spark_session.sql(f"SELECT * FROM {test_table_path}")
    assert written_df.count() == 5

    print(f"✓ Verified {written_df.count()} rows in Delta table")

    # Cleanup
    databricks_spark_session.sql(f"DROP TABLE IF EXISTS {test_table_path}")


def test_write_to_delta_with_validation_fail(
    databricks_spark_session, sample_databricks_df, test_table_path
):
    """CRITICAL: Test that validation failure prevents write on Databricks."""
    # Create failing validation result
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")
    validation_result.add_failed("rule2", "Data quality check failed")

    target_config = {
        "type": "delta",
        "path": test_table_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()

    # Should raise ValidationError
    with pytest.raises(ValidationError) as exc_info:
        writer.write(sample_databricks_df, target_config, validation_result)

    assert "validation failed" in str(exc_info.value).lower()
    print(f"\n✓ Validation correctly prevented write")

    # Verify NO data was written
    try:
        databricks_spark_session.sql(f"SELECT * FROM {test_table_path}")
        assert False, "Table should not exist"
    except Exception as e:
        print(f"✓ Confirmed table does not exist (expected)")


def test_validation_orchestrator_on_databricks(
    databricks_spark_session, sample_databricks_df
):
    """Test ValidationOrchestrator with Databricks."""
    validation_config = {
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
                "name": "check_range",
                "type": "range_check",
                "column": "age",
                "min_value": 20,
                "max_value": 40,
            },
            {
                "name": "check_count",
                "type": "row_count",
                "min_count": 1,
                "max_count": 100,
            },
        ],
    }

    orchestrator = ValidationOrchestrator()
    result = orchestrator.validate(sample_databricks_df, validation_config)

    assert result.all_passed() is True
    assert len(result.rules_executed) == 3
    assert len(result.rules_failed) == 0

    print(f"\n✓ All validation rules passed on Databricks")
    print(f"  - Rules executed: {len(result.rules_executed)}")
    print(f"  - Rules passed: {len(result.rules_passed)}")


def test_write_with_partitions_on_databricks(
    databricks_spark_session, sample_databricks_df, test_table_path
):
    """Test partitioned write to Delta Lake on Databricks."""
    validation_result = ValidationResult()
    validation_result.add_passed("rule1")

    target_config = {
        "type": "delta",
        "path": test_table_path,
        "mode": "overwrite",
        "partition_by": ["date"],
    }

    writer = DeltaWriter()
    result = writer.write(sample_databricks_df, target_config, validation_result)

    assert result.success is True
    print(f"\n✓ Successfully wrote partitioned data to: {test_table_path}")

    # Verify partitions
    written_df = databricks_spark_session.sql(f"SELECT * FROM {test_table_path}")
    assert written_df.count() == 5

    # Check partition info
    partitions = databricks_spark_session.sql(
        f"SHOW PARTITIONS {test_table_path}"
    ).count()
    print(f"✓ Created {partitions} partitions")

    # Cleanup
    databricks_spark_session.sql(f"DROP TABLE IF EXISTS {test_table_path}")


def test_end_to_end_pipeline_on_databricks(
    databricks_spark_session, test_schema, test_table_path
):
    """Test complete pipeline flow on Databricks.

    Steps:
    1. Create source data
    2. Apply transformation
    3. Run validation
    4. Write to Delta (only if validation passes)
    """
    # Step 1: Create source data
    source_data = databricks_spark_session.createDataFrame(
        [
            (1, "Alice", 25, "engineering", "2024-01-01"),
            (2, "Bob", 30, "sales", "2024-01-01"),
            (3, "Charlie", 35, "engineering", "2024-01-02"),
        ],
        ["id", "name", "age", "department", "date"],
    )

    print(f"\n✓ Created source data with {source_data.count()} rows")

    # Step 2: Apply transformation (filter)
    from pyspark.sql import functions as F

    transformed = source_data.filter(F.col("department") == "engineering")

    print(f"✓ Applied transformation, {transformed.count()} rows remaining")

    # Step 3: Run validation
    validation_config = {
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
                "name": "check_count",
                "type": "row_count",
                "min_count": 1,
            },
        ],
    }

    orchestrator = ValidationOrchestrator()
    validation_result = orchestrator.validate(transformed, validation_config)

    assert validation_result.all_passed() is True
    print(f"✓ Validation passed: {len(validation_result.rules_passed)} rules")

    # Step 4: Write to Delta
    target_config = {
        "type": "delta",
        "path": test_table_path,
        "mode": "overwrite",
        "partition_by": ["date"],
    }

    writer = DeltaWriter()
    write_result = writer.write(transformed, target_config, validation_result)

    assert write_result.success is True
    assert write_result.rows_written == 2  # Only engineering rows

    print(f"✓ Successfully wrote {write_result.rows_written} rows to Delta")

    # Verify final result
    final_df = databricks_spark_session.sql(f"SELECT * FROM {test_table_path}")
    assert final_df.count() == 2

    departments = [row.department for row in final_df.collect()]
    assert all(dept == "engineering" for dept in departments)

    print(f"✓ End-to-end pipeline completed successfully!")
    print(f"  - Source rows: 3")
    print(f"  - After transformation: 2")
    print(f"  - Final rows in Delta: {final_df.count()}")

    # Cleanup
    databricks_spark_session.sql(f"DROP TABLE IF EXISTS {test_table_path}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
