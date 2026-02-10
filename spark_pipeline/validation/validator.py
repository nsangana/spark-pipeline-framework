"""Validation orchestrator."""

from typing import Dict, List

from pyspark.sql import DataFrame

from spark_pipeline.validation.builtin_rules import ValidationRuleRegistry
from spark_pipeline.validation.rules import ValidationResult, ValidationRule
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)


class ValidationOrchestrator:
    """Orchestrates execution of validation rules."""

    def __init__(self):
        """Initialize validation orchestrator."""
        self.rule_registry = ValidationRuleRegistry()

    def validate(
        self, df: DataFrame, validation_config: Dict
    ) -> ValidationResult:
        """Execute all validation rules on a DataFrame.

        Args:
            df: DataFrame to validate
            validation_config: Validation configuration with keys:
                - enabled: Whether validation is enabled
                - fail_on_error: Whether to fail on validation errors
                - rules: List of rule configurations

        Returns:
            ValidationResult with pass/fail status and metrics
        """
        result = ValidationResult()

        # Check if validation is enabled
        if not validation_config.get("enabled", True):
            logger.info("Validation is disabled, skipping")
            return result

        rules_config = validation_config.get("rules", [])

        if not rules_config:
            logger.info("No validation rules configured")
            return result

        logger.info(f"Running {len(rules_config)} validation rules")

        # Execute each rule
        for rule_config in rules_config:
            rule = self._create_rule(rule_config)

            try:
                passed, message = rule.validate(df)

                if passed:
                    result.add_passed(rule.name)
                else:
                    result.add_failed(rule.name, message)

                # Store metrics
                result.add_metric(rule.name, {"passed": passed, "message": message})

            except Exception as e:
                error_msg = f"Error executing rule '{rule.name}': {e}"
                logger.error(error_msg)
                result.add_failed(rule.name, error_msg)

        # Log summary
        self._log_summary(result)

        return result

    def _create_rule(self, rule_config: Dict) -> ValidationRule:
        """Create a validation rule from configuration.

        Args:
            rule_config: Rule configuration

        Returns:
            ValidationRule instance
        """
        return self.rule_registry.create(rule_config)

    def _log_summary(self, result: ValidationResult) -> None:
        """Log validation summary.

        Args:
            result: Validation result
        """
        logger.info("=" * 60)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total rules executed: {len(result.rules_executed)}")
        logger.info(f"Rules passed: {len(result.rules_passed)}")
        logger.info(f"Rules failed: {len(result.rules_failed)}")

        if result.rules_failed:
            logger.error("\nFailed rules:")
            for rule_name in result.rules_failed:
                reason = result.failure_details.get(rule_name, "Unknown reason")
                logger.error(f"  - {rule_name}: {reason}")

        status = "PASSED ✓" if result.all_passed() else "FAILED ✗"
        logger.info(f"\nOverall status: {status}")
        logger.info("=" * 60)
