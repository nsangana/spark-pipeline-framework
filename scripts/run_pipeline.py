#!/usr/bin/env python3
"""CLI script for running pipelines."""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from spark_pipeline.core.engine import PipelineEngine
from spark_pipeline.utils.logging_config import setup_logging


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Run a Spark declarative pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "config",
        type=str,
        help="Path to pipeline YAML configuration file",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-file",
        type=str,
        help="Optional log file path",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=args.log_level, log_file=args.log_file)

    # Run pipeline
    try:
        engine = PipelineEngine(args.config)
        result = engine.execute()

        if result.status == "success":
            print("\n✓ Pipeline executed successfully!")
            print(f"  Pipeline: {result.pipeline_name}")
            print(f"  Rows written: {result.metrics.get('rows_written', 'N/A')}")
            print(f"  Validation passed: {result.validation_passed}")
            sys.exit(0)
        else:
            print(f"\n✗ Pipeline execution failed: {result.error}")
            sys.exit(1)

    except Exception as e:
        print(f"\n✗ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
