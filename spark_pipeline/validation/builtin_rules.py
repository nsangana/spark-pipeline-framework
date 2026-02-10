"""Built-in validation rules."""

from typing import Dict, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_pipeline.validation.rules import ValidationRule
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class NullCheckRule(ValidationRule):
    """Check null percentage in a column against a threshold."""

    def __init__(self, config: Dict):
        """Initialize null check rule.

        Args:
            config: Configuration with 'column' and 'threshold' keys
        """
        super().__init__(config)
        self.column = config["column"]
        self.threshold = config.get("threshold", 0.0)

    def validate(self, df: DataFrame) -> tuple[bool, str]:
        """Validate null percentage.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (passed, message)
        """
        total_count = df.count()

        if total_count == 0:
            return True, f"Column '{self.column}' has no rows to validate"

        null_count = df.filter(F.col(self.column).isNull()).count()
        null_percentage = (null_count / total_count) * 100

        passed = null_percentage <= self.threshold
        message = (
            f"Column '{self.column}' null percentage: {null_percentage:.2f}% "
            f"(threshold: {self.threshold}%)"
        )

        if passed:
            logger.info(f"✓ {message}")
        else:
            logger.error(f"✗ {message}")

        return passed, message


class RangeCheckRule(ValidationRule):
    """Check if numeric values are within a specified range."""

    def __init__(self, config: Dict):
        """Initialize range check rule.

        Args:
            config: Configuration with 'column', 'min_value', 'max_value' keys
        """
        super().__init__(config)
        self.column = config["column"]
        self.min_value = config.get("min_value")
        self.max_value = config.get("max_value")

    def validate(self, df: DataFrame) -> tuple[bool, str]:
        """Validate value range.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (passed, message)
        """
        # Build condition
        condition = F.col(self.column).isNotNull()

        if self.min_value is not None:
            condition = condition & (F.col(self.column) >= self.min_value)

        if self.max_value is not None:
            condition = condition & (F.col(self.column) <= self.max_value)

        # Count violations
        total_count = df.filter(F.col(self.column).isNotNull()).count()
        valid_count = df.filter(condition).count()
        invalid_count = total_count - valid_count

        passed = invalid_count == 0

        range_str = f"[{self.min_value}, {self.max_value}]"
        message = (
            f"Column '{self.column}' range check {range_str}: "
            f"{invalid_count} violations out of {total_count} rows"
        )

        if passed:
            logger.info(f"✓ {message}")
        else:
            logger.error(f"✗ {message}")

        return passed, message


class RowCountRule(ValidationRule):
    """Check if row count is within expected bounds."""

    def __init__(self, config: Dict):
        """Initialize row count rule.

        Args:
            config: Configuration with 'min_count' and/or 'max_count' keys
        """
        super().__init__(config)
        self.min_count = config.get("min_count")
        self.max_count = config.get("max_count")

    def validate(self, df: DataFrame) -> tuple[bool, str]:
        """Validate row count.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (passed, message)
        """
        row_count = df.count()
        passed = True

        if self.min_count is not None and row_count < self.min_count:
            passed = False
            message = f"Row count {row_count} is below minimum {self.min_count}"
        elif self.max_count is not None and row_count > self.max_count:
            passed = False
            message = f"Row count {row_count} exceeds maximum {self.max_count}"
        else:
            bounds = []
            if self.min_count is not None:
                bounds.append(f"min={self.min_count}")
            if self.max_count is not None:
                bounds.append(f"max={self.max_count}")
            bounds_str = ", ".join(bounds)
            message = f"Row count {row_count} is within bounds ({bounds_str})"

        if passed:
            logger.info(f"✓ {message}")
        else:
            logger.error(f"✗ {message}")

        return passed, message


class ValidationRuleRegistry:
    """Registry for validation rule types."""

    _rules: Dict[str, Type[ValidationRule]] = {
        "null_check": NullCheckRule,
        "range_check": RangeCheckRule,
        "row_count": RowCountRule,
    }

    @classmethod
    def register(cls, rule_type: str, rule_class: Type[ValidationRule]) -> None:
        """Register a rule type.

        Args:
            rule_type: Type identifier
            rule_class: Rule class
        """
        cls._rules[rule_type] = rule_class
        logger.debug(f"Registered validation rule type: {rule_type}")

    @classmethod
    def create(cls, config: Dict) -> ValidationRule:
        """Create a validation rule from configuration.

        Args:
            config: Rule configuration

        Returns:
            ValidationRule instance

        Raises:
            ValueError: If rule type not found
        """
        rule_type = config.get("type")

        if rule_type not in cls._rules:
            raise ValueError(
                f"Unknown validation rule type: {rule_type}. "
                f"Available types: {list(cls._rules.keys())}"
            )

        rule_class = cls._rules[rule_type]
        return rule_class(config)

    @classmethod
    def list_types(cls) -> list[str]:
        """List all registered rule types.

        Returns:
            List of rule type names
        """
        return list(cls._rules.keys())
