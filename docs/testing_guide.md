# Testing Guide

Comprehensive guide to testing Spark pipelines.

## Testing Strategy

The framework uses a three-tier testing approach:

1. **Unit Tests** - Test individual components in isolation
2. **Integration Tests** - Test complete pipeline execution
3. **Data Quality Tests** - Test data validation rules

## Setting Up Tests

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── unit/                    # Unit tests
│   ├── test_config_parser.py
│   ├── test_transformations.py
│   ├── test_validator.py
│   └── test_writer.py
├── integration/             # Integration tests
│   └── test_pipeline_execution.py
└── data_quality/           # Data quality tests
    └── test_user_analytics_quality.py
```

### Running Tests

```bash
# All tests
make test

# Specific test file
pytest tests/unit/test_transformations.py -v

# Specific test
pytest tests/unit/test_transformations.py::test_sql_transformation -v

# With coverage
make test-coverage
```

## Unit Tests

Test individual components in isolation.

### Testing Transformations

```python
# tests/unit/test_my_transforms.py

import pytest
from pyspark.sql import SparkSession
from transformations.my_transforms import add_timestamp_features

def test_add_timestamp_features(spark_session):
    """Test timestamp feature extraction."""
    # Arrange
    input_df = spark_session.createDataFrame([
        (1, "2024-01-15 10:30:00"),
        (2, "2024-02-20 15:45:00"),
    ], ["id", "timestamp"])

    # Act
    result = add_timestamp_features(input_df)

    # Assert
    assert "date" in result.columns
    assert "hour" in result.columns
    assert "day_of_week" in result.columns
    assert result.count() == 2

    # Check specific values
    first_row = result.filter("id = 1").collect()[0]
    assert first_row["hour"] == 10
```

### Testing with Chispa

[Chispa](https://github.com/MrPowers/chispa) provides DataFrame assertion helpers:

```python
from chispa.dataframe_comparer import assert_df_equality

def test_transformation_output(spark_session):
    """Test transformation produces expected output."""
    input_df = spark_session.createDataFrame([
        (1, "Alice", 25),
        (2, "Bob", 30),
    ], ["id", "name", "age"])

    expected_df = spark_session.createDataFrame([
        (1, "Alice", 25, "adult"),
        (2, "Bob", 30, "adult"),
    ], ["id", "name", "age", "category"])

    result = add_age_category(input_df)

    assert_df_equality(result, expected_df, ignore_row_order=True)
```

### Testing Validation Rules

```python
# tests/unit/test_my_validators.py

def test_null_check_rule_passes(spark_session, sample_dataframe):
    """Test null check rule with clean data."""
    from spark_pipeline.validation.builtin_rules import NullCheckRule

    config = {
        "name": "check_id_nulls",
        "type": "null_check",
        "column": "id",
        "threshold": 0.0,
    }

    rule = NullCheckRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is True
    assert "0.00%" in message


def test_null_check_rule_fails(spark_session):
    """Test null check rule with nulls."""
    df = spark_session.createDataFrame([
        (1, "Alice"),
        (None, "Bob"),
        (None, "Charlie"),
    ], ["id", "name"])

    config = {
        "name": "check_nulls",
        "type": "null_check",
        "column": "id",
        "threshold": 10.0,  # 10% threshold
    }

    rule = NullCheckRule(config)
    passed, message = rule.validate(df)

    assert passed is False  # 67% nulls > 10%
```

### Testing Writer Conditional Logic

Critical test to verify data is NOT written when validation fails:

```python
def test_writer_prevents_write_on_validation_failure(
    spark_session, sample_dataframe, temp_delta_path
):
    """CRITICAL: Test that failed validation prevents write."""
    from spark_pipeline.core.writer import DeltaWriter
    from spark_pipeline.validation.rules import ValidationResult
    from spark_pipeline.core.exceptions import ValidationError

    # Create failing validation result
    validation_result = ValidationResult()
    validation_result.add_failed("rule1", "Data quality check failed")

    target_config = {
        "type": "delta",
        "path": temp_delta_path,
        "mode": "overwrite",
    }

    writer = DeltaWriter()

    # Should raise ValidationError
    with pytest.raises(ValidationError) as exc_info:
        writer.write(sample_dataframe, target_config, validation_result)

    assert "validation failed" in str(exc_info.value).lower()

    # Verify NO data was written
    try:
        spark_session.read.format("delta").load(temp_delta_path)
        assert False, "Data should not have been written"
    except Exception:
        pass  # Expected - table should not exist
```

## Integration Tests

Test complete pipeline execution end-to-end.

### Basic Pipeline Test

```python
# tests/integration/test_my_pipeline.py

import yaml
from pathlib import Path
from spark_pipeline.core.engine import PipelineEngine

def test_complete_pipeline_execution(spark_session, temp_dir):
    """Test full pipeline execution."""
    # Create input data
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    sample_data = spark_session.createDataFrame([
        (1, "Alice", 25),
        (2, "Bob", 30),
    ], ["id", "name", "age"])

    sample_data.write.parquet(str(input_path))

    # Create pipeline config
    config = {
        "pipeline": {"name": "test_pipeline"},
        "sources": [
            {"name": "users", "type": "parquet", "path": str(input_path)}
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

    # Assertions
    assert result.status == "success"
    assert result.validation_passed is True
    assert result.metrics["rows_written"] == 2

    # Verify output
    output_df = spark_session.read.format("delta").load(str(output_path))
    assert output_df.count() == 2
```

### Testing Validation Gates

```python
def test_pipeline_fails_with_bad_data(spark_session, temp_dir):
    """Test pipeline correctly fails with validation errors."""
    # Create data with nulls
    input_path = temp_dir / "input"
    output_path = temp_dir / "output"

    bad_data = spark_session.createDataFrame([
        (1, "Alice"),
        (None, "Bob"),  # Null ID
    ], ["id", "name"])

    bad_data.write.parquet(str(input_path))

    # Config with strict validation
    config = {
        "pipeline": {"name": "test_pipeline"},
        "sources": [
            {"name": "data", "type": "parquet", "path": str(input_path)}
        ],
        "validation": {
            "enabled": True,
            "fail_on_error": True,
            "rules": [
                {
                    "name": "check_nulls",
                    "type": "null_check",
                    "column": "id",
                    "threshold": 0.0,  # No nulls allowed
                }
            ],
        },
        "target": {
            "type": "delta",
            "path": str(output_path),
            "mode": "overwrite",
        },
    }

    config_path = temp_dir / "pipeline.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    # Execute pipeline
    engine = PipelineEngine(str(config_path))
    result = engine.execute()

    # Should fail
    assert result.status == "failed"
    assert result.validation_passed is False

    # Verify NO data written
    try:
        spark_session.read.format("delta").load(str(output_path))
        assert False, "Table should not exist"
    except Exception:
        pass  # Expected
```

## Data Quality Tests

Test domain-specific data quality rules.

### Using pytest Markers

```python
# tests/data_quality/test_my_data_quality.py

import pytest

@pytest.mark.data_quality
def test_revenue_is_positive(spark_session):
    """Test that all revenue values are positive."""
    # This would run on actual pipeline data
    test_df = spark_session.createDataFrame([
        (1, 100.0),
        (2, 250.0),
    ], ["id", "revenue"])

    from pyspark.sql import functions as F

    negative_revenue = test_df.filter(F.col("revenue") < 0)
    assert negative_revenue.count() == 0, "Found negative revenue values"


@pytest.mark.data_quality
def test_user_segments_valid(spark_session):
    """Test that user segments are from expected set."""
    valid_segments = {"free", "premium", "enterprise"}

    test_df = spark_session.createDataFrame([
        (1, "free"),
        (2, "premium"),
        (3, "enterprise"),
    ], ["user_id", "segment"])

    from pyspark.sql import functions as F

    unique_segments = {
        row.segment for row in test_df.select("segment").distinct().collect()
    }

    invalid = unique_segments - valid_segments
    assert len(invalid) == 0, f"Invalid segments: {invalid}"
```

### Run Only Data Quality Tests

```bash
pytest -m data_quality tests/
```

## Testing Best Practices

### 1. Use Fixtures

Leverage pytest fixtures for reusable test data:

```python
# conftest.py

@pytest.fixture
def sample_events_df(spark_session):
    """Sample events DataFrame for testing."""
    return spark_session.createDataFrame([
        (1, 1, "page_view", "2024-01-01 10:00:00"),
        (2, 1, "click", "2024-01-01 10:05:00"),
        (3, 2, "purchase", "2024-01-01 11:00:00"),
    ], ["event_id", "user_id", "event_type", "timestamp"])
```

### 2. Test Edge Cases

```python
def test_handle_empty_dataframe(spark_session):
    """Test transformation handles empty input."""
    empty_df = spark_session.createDataFrame([], "id INT, name STRING")

    result = my_transformation(empty_df)

    assert result.count() == 0
    assert "expected_column" in result.columns


def test_handle_null_values(spark_session):
    """Test transformation handles nulls gracefully."""
    df_with_nulls = spark_session.createDataFrame([
        (1, None),
        (None, "test"),
    ], ["id", "name"])

    result = my_transformation(df_with_nulls)
    # Assert expected behavior with nulls
```

### 3. Test Data Types

```python
def test_output_schema(spark_session):
    """Test that output has expected schema."""
    from pyspark.sql.types import IntegerType, StringType

    input_df = spark_session.createDataFrame([
        (1, "Alice"),
    ], ["id", "name"])

    result = my_transformation(input_df)

    # Check schema
    assert result.schema["id"].dataType == IntegerType()
    assert result.schema["new_column"].dataType == StringType()
```

### 4. Parameterized Tests

Test multiple scenarios efficiently:

```python
@pytest.mark.parametrize("threshold,expected_count", [
    (0, 5),
    (100, 3),
    (500, 1),
])
def test_filter_by_threshold(spark_session, threshold, expected_count):
    """Test filtering with different thresholds."""
    df = spark_session.createDataFrame([
        (1, 50),
        (2, 150),
        (3, 300),
        (4, 600),
        (5, 1000),
    ], ["id", "value"])

    result = filter_by_value(df, threshold)
    assert result.count() == expected_count
```

### 5. Mock External Dependencies

```python
from unittest.mock import patch, MagicMock

def test_transformation_with_external_api():
    """Test transformation that calls external API."""
    with patch('my_module.call_external_api') as mock_api:
        mock_api.return_value = {"result": "mocked_data"}

        result = my_transformation_with_api(test_df)

        # Verify API was called
        mock_api.assert_called_once()
        # Assert transformation result
        assert result.count() > 0
```

## Performance Testing

### Benchmark Tests

```python
import time

def test_transformation_performance(spark_session):
    """Test that transformation completes within time limit."""
    large_df = spark_session.range(1000000)

    start = time.time()
    result = my_transformation(large_df)
    result.count()  # Trigger execution
    duration = time.time() - start

    assert duration < 10.0, f"Transformation took {duration}s, expected <10s"
```

### Memory Tests

```python
def test_no_memory_explosion(spark_session):
    """Test transformation doesn't cause memory issues."""
    # Create large dataset
    df = spark_session.range(1000000).toDF("id")

    # Should not cache or collect large data
    result = my_transformation(df)

    # Just verify it runs without OOM
    assert result.count() > 0
```

## Continuous Integration

### GitHub Actions Example

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - uses: actions/setup-java@v2
        with:
          distribution: 'adopt'
          java-version: '11'

      - name: Install dependencies
        run: |
          pip install -r requirements-dev.txt
          pip install -e .

      - name: Run tests
        run: make test-coverage

      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## Debugging Failed Tests

### 1. Show DataFrame Content

```python
def test_debug_transformation(spark_session):
    """Debug test by showing data."""
    df = create_test_data(spark_session)

    print("\nInput data:")
    df.show()

    result = my_transformation(df)

    print("\nOutput data:")
    result.show()
    print("\nOutput schema:")
    result.printSchema()

    # Your assertions
```

### 2. Use explain() for Query Plans

```python
def test_debug_query_plan(spark_session):
    """Debug by examining query plan."""
    df = create_test_data(spark_session)
    result = my_transformation(df)

    print("\nQuery plan:")
    result.explain(extended=True)
```

### 3. Increase Logging

```python
import logging

def test_with_debug_logging(spark_session, caplog):
    """Test with debug logging enabled."""
    caplog.set_level(logging.DEBUG)

    result = my_transformation(test_df)

    # Check logs
    assert "Expected log message" in caplog.text
```

## Next Steps

- Review [Configuration Guide](configuration_guide.md)
- Check [Transformation Guide](transformation_guide.md)
- Explore [Examples](../examples/user_analytics/README.md)
