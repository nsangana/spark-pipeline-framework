"""Unit tests for transformations."""

import pytest

from spark_pipeline.core.context import PipelineContext
from spark_pipeline.core.exceptions import TransformationError
from spark_pipeline.transformations.builtin import (
    AggregateTransformation,
    FilterTransformation,
    JoinTransformation,
)
from spark_pipeline.transformations.registry import TransformationRegistry
from spark_pipeline.transformations.sql import SQLTransformation


def test_sql_transformation(spark_session, sample_dataframe):
    """Test SQL transformation."""
    context = PipelineContext(spark_session)
    context.register_dataframe("test_data", sample_dataframe)

    config = {
        "name": "filter_high_salary",
        "type": "sql",
        "inputs": ["test_data"],
        "output": "high_salary",
        "sql": "SELECT * FROM test_data WHERE salary > 60000",
    }

    transformation = SQLTransformation(config)
    result = transformation.execute(context)

    assert result.count() == 2  # Charlie and Eve
    assert "name" in result.columns


def test_filter_transformation(spark_session, sample_dataframe):
    """Test filter transformation."""
    context = PipelineContext(spark_session)
    context.register_dataframe("test_data", sample_dataframe)

    config = {
        "name": "filter_age",
        "type": "filter",
        "inputs": ["test_data"],
        "output": "filtered",
        "condition": "age >= 30",
    }

    transformation = FilterTransformation(config)
    result = transformation.execute(context)

    assert result.count() == 3  # Bob, Charlie, Eve


def test_join_transformation(spark_session):
    """Test join transformation."""
    # Create two DataFrames
    df1 = spark_session.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        ["id", "name"],
    )

    df2 = spark_session.createDataFrame(
        [(1, "Engineering"), (2, "Sales"), (3, "Marketing")],
        ["id", "department"],
    )

    context = PipelineContext(spark_session)
    context.register_dataframe("employees", df1)
    context.register_dataframe("departments", df2)

    config = {
        "name": "join_emp_dept",
        "type": "join",
        "inputs": ["employees", "departments"],
        "output": "joined",
        "on": "id",
        "how": "inner",
    }

    transformation = JoinTransformation(config)
    result = transformation.execute(context)

    assert result.count() == 3
    assert "name" in result.columns
    assert "department" in result.columns


def test_aggregate_transformation(spark_session, sample_dataframe):
    """Test aggregate transformation."""
    context = PipelineContext(spark_session)
    context.register_dataframe("test_data", sample_dataframe)

    config = {
        "name": "aggregate_by_date",
        "type": "aggregate",
        "inputs": ["test_data"],
        "output": "aggregated",
        "group_by": ["date"],
        "aggregations": {
            "salary": "avg",
            "id": "count",
        },
    }

    transformation = AggregateTransformation(config)
    result = transformation.execute(context)

    assert result.count() == 3  # 3 unique dates
    assert "salary_avg" in result.columns
    assert "id_count" in result.columns


def test_transformation_registry():
    """Test transformation registry."""
    # Check that transformations are registered
    types = TransformationRegistry.list_types()

    assert "sql" in types
    assert "python" in types
    assert "filter" in types
    assert "join" in types
    assert "aggregate" in types


def test_create_transformation_from_config():
    """Test creating transformation from config."""
    config = {
        "name": "test_transform",
        "type": "sql",
        "inputs": ["source"],
        "output": "result",
        "sql": "SELECT * FROM source",
    }

    transformation = TransformationRegistry.create(config)

    assert isinstance(transformation, SQLTransformation)
    assert transformation.name == "test_transform"


def test_create_unknown_transformation():
    """Test creating unknown transformation type."""
    config = {
        "name": "test_transform",
        "type": "unknown_type",
        "inputs": ["source"],
        "output": "result",
    }

    with pytest.raises(TransformationError):
        TransformationRegistry.create(config)
