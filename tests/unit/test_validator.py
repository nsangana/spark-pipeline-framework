"""Unit tests for validation framework."""

import pytest

from spark_pipeline.validation.builtin_rules import (
    NullCheckRule,
    RangeCheckRule,
    RowCountRule,
    ValidationRuleRegistry,
)
from spark_pipeline.validation.validator import ValidationOrchestrator


def test_null_check_rule_pass(spark_session, sample_dataframe):
    """Test null check rule that passes."""
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


def test_null_check_rule_fail(spark_session):
    """Test null check rule that fails."""
    # Create DataFrame with nulls
    df = spark_session.createDataFrame(
        [(1, "Alice"), (2, None), (3, "Charlie"), (4, None)],
        ["id", "name"],
    )

    config = {
        "name": "check_name_nulls",
        "type": "null_check",
        "column": "name",
        "threshold": 10.0,  # Allow max 10% nulls
    }

    rule = NullCheckRule(config)
    passed, message = rule.validate(df)

    assert passed is False  # 50% nulls > 10% threshold


def test_range_check_rule_pass(spark_session, sample_dataframe):
    """Test range check rule that passes."""
    config = {
        "name": "check_age_range",
        "type": "range_check",
        "column": "age",
        "min_value": 20,
        "max_value": 40,
    }

    rule = RangeCheckRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is True
    assert "0 violations" in message


def test_range_check_rule_fail(spark_session, sample_dataframe):
    """Test range check rule that fails."""
    config = {
        "name": "check_age_range",
        "type": "range_check",
        "column": "age",
        "min_value": 20,
        "max_value": 30,  # Charlie (35) and Eve (32) are outside range
    }

    rule = RangeCheckRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is False
    assert "violations" in message


def test_row_count_rule_pass(spark_session, sample_dataframe):
    """Test row count rule that passes."""
    config = {
        "name": "check_row_count",
        "type": "row_count",
        "min_count": 3,
        "max_count": 10,
    }

    rule = RowCountRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is True


def test_row_count_rule_fail_min(spark_session, sample_dataframe):
    """Test row count rule that fails on min."""
    config = {
        "name": "check_row_count",
        "type": "row_count",
        "min_count": 10,
    }

    rule = RowCountRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is False
    assert "below minimum" in message


def test_row_count_rule_fail_max(spark_session, sample_dataframe):
    """Test row count rule that fails on max."""
    config = {
        "name": "check_row_count",
        "type": "row_count",
        "max_count": 3,
    }

    rule = RowCountRule(config)
    passed, message = rule.validate(sample_dataframe)

    assert passed is False
    assert "exceeds maximum" in message


def test_validation_orchestrator_all_pass(spark_session, sample_dataframe):
    """Test validation orchestrator with all rules passing."""
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
    result = orchestrator.validate(sample_dataframe, validation_config)

    assert result.all_passed() is True
    assert len(result.rules_executed) == 3
    assert len(result.rules_passed) == 3
    assert len(result.rules_failed) == 0


def test_validation_orchestrator_some_fail(spark_session, sample_dataframe):
    """Test validation orchestrator with some rules failing."""
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
                "max_value": 27,  # Will fail for most rows
            },
        ],
    }

    orchestrator = ValidationOrchestrator()
    result = orchestrator.validate(sample_dataframe, validation_config)

    assert result.all_passed() is False
    assert len(result.rules_executed) == 2
    assert len(result.rules_passed) == 1
    assert len(result.rules_failed) == 1
    assert "check_range" in result.rules_failed


def test_validation_disabled(spark_session, sample_dataframe):
    """Test validation when disabled."""
    validation_config = {
        "enabled": False,
        "rules": [],
    }

    orchestrator = ValidationOrchestrator()
    result = orchestrator.validate(sample_dataframe, validation_config)

    assert result.all_passed() is True
    assert len(result.rules_executed) == 0


def test_validation_rule_registry():
    """Test validation rule registry."""
    types = ValidationRuleRegistry.list_types()

    assert "null_check" in types
    assert "range_check" in types
    assert "row_count" in types


def test_create_rule_from_config():
    """Test creating rule from configuration."""
    config = {
        "name": "test_rule",
        "type": "null_check",
        "column": "test_column",
        "threshold": 5.0,
    }

    rule = ValidationRuleRegistry.create(config)

    assert isinstance(rule, NullCheckRule)
    assert rule.name == "test_rule"
    assert rule.column == "test_column"
