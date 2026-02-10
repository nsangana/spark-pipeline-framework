#!/usr/bin/env python3
"""Utility script to validate pipeline configuration files."""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from spark_pipeline.core.config_parser import ConfigParser
from spark_pipeline.core.exceptions import PipelineConfigError


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate a pipeline YAML configuration file"
    )

    parser.add_argument(
        "config",
        type=str,
        help="Path to pipeline YAML configuration file",
    )

    args = parser.parse_args()

    print(f"Validating configuration: {args.config}")
    print("-" * 60)

    try:
        # Parse and validate config
        config_parser = ConfigParser()
        config = config_parser.parse(args.config)

        # Print summary
        print("✓ Configuration is valid!\n")
        print(f"Pipeline name: {config.name}")
        print(f"Sources: {len(config.sources)}")
        print(f"Transformations: {len(config.transformations)}")
        print(f"Validation enabled: {config.validation_config.get('enabled', False)}")
        print(f"Target type: {config.target['type']}")
        print(f"Target path: {config.target['path']}")

        if config.transformations:
            print("\nTransformations:")
            for i, t in enumerate(config.transformations, 1):
                print(f"  {i}. {t['name']} ({t['type']})")

        if config.validation_config.get("rules"):
            print(f"\nValidation rules: {len(config.validation_config['rules'])}")
            for rule in config.validation_config["rules"]:
                print(f"  - {rule['name']} ({rule['type']})")

        sys.exit(0)

    except PipelineConfigError as e:
        print(f"\n✗ Configuration validation failed:")
        print(f"  {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error:")
        print(f"  {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
