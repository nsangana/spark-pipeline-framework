"""Base classes for validation rules and results."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List

from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    """Result of validation execution.

    Contains information about which rules passed/failed and detailed metrics.
    """

    rules_executed: List[str] = field(default_factory=list)
    rules_passed: List[str] = field(default_factory=list)
    rules_failed: List[str] = field(default_factory=list)
    failure_details: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def all_passed(self) -> bool:
        """Check if all validation rules passed.

        Returns:
            True if all rules passed, False otherwise
        """
        return len(self.rules_failed) == 0

    def get_failures(self) -> List[str]:
        """Get list of failed rule names.

        Returns:
            List of failed rule names
        """
        return self.rules_failed

    def add_passed(self, rule_name: str) -> None:
        """Mark a rule as passed.

        Args:
            rule_name: Name of the rule
        """
        self.rules_executed.append(rule_name)
        self.rules_passed.append(rule_name)

    def add_failed(self, rule_name: str, reason: str) -> None:
        """Mark a rule as failed.

        Args:
            rule_name: Name of the rule
            reason: Reason for failure
        """
        self.rules_executed.append(rule_name)
        self.rules_failed.append(rule_name)
        self.failure_details[rule_name] = reason

    def add_metric(self, key: str, value: Any) -> None:
        """Add a metric.

        Args:
            key: Metric name
            value: Metric value
        """
        self.metrics[key] = value

    def __repr__(self) -> str:
        """String representation."""
        status = "PASSED" if self.all_passed() else "FAILED"
        return (
            f"ValidationResult(status={status}, "
            f"executed={len(self.rules_executed)}, "
            f"passed={len(self.rules_passed)}, "
            f"failed={len(self.rules_failed)})"
        )


class ValidationRule(ABC):
    """Abstract base class for validation rules."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize validation rule.

        Args:
            config: Rule configuration
        """
        self.config = config
        self.name = config["name"]
        self.rule_type = config["type"]

    @abstractmethod
    def validate(self, df: DataFrame) -> tuple[bool, str]:
        """Execute validation rule.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (passed: bool, message: str)
        """
        pass

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name={self.name})"
