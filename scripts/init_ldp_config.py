#!/usr/bin/env python3
"""
Initialize LDP configuration for the current environment.

This script creates a configuration file with settings detected from your
Databricks CLI profile or environment variables.
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from spark_pipeline.ldp_generator import load_config, create_default_config_file


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Initialize LDP configuration"
    )
    parser.add_argument(
        "--output",
        default="ldp_config.yaml",
        help="Output config file path (default: ldp_config.yaml)",
    )
    parser.add_argument(
        "--profile",
        help="Databricks profile to use",
    )
    parser.add_argument(
        "--catalog",
        help="Unity Catalog name",
    )
    parser.add_argument(
        "--schema",
        help="Schema name",
    )
    parser.add_argument(
        "--detect",
        action="store_true",
        help="Auto-detect settings from current environment",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("LDP Configuration Initializer")
    print("=" * 70)

    if args.detect:
        # Load configuration from environment and save to file
        print(f"\n🔍 Detecting configuration from environment...")

        config = load_config(
            profile=args.profile,
            catalog=args.catalog,
            schema=args.schema,
        )

        print(f"\n✓ Detected settings:")
        print(f"  Profile: {config.profile}")
        print(f"  Catalog: {config.catalog}")
        print(f"  Schema: {config.schema}")
        print(f"  Workspace User: {config.get_workspace_user()}")
        print(f"  Workspace Path: {config.get_workspace_base_path()}")
        print(f"  Serverless: {config.serverless}")
        print(f"  Edition: {config.edition}")
        print(f"  Development Mode: {config.development}")

        # Save to YAML
        import yaml

        config_data = {
            "ldp": {
                "profile": config.profile,
                "catalog": config.catalog,
                "schema": config.schema,
                "workspace_user": config.workspace_user,
                "workspace_base_path": config.workspace_base_path,
                "serverless": config.serverless,
                "channel": config.channel,
                "edition": config.edition,
                "development": config.development,
            }
        }

        with open(args.output, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        print(f"\n✅ Configuration saved to: {args.output}")

    else:
        # Create default template
        create_default_config_file(args.output)

    print(f"\n💡 Usage:")
    print(f"  1. Edit {args.output} to customize settings")
    print(f"  2. Set environment variables:")
    print(f"     export DATABRICKS_PROFILE=<profile>")
    print(f"     export DATABRICKS_CATALOG=<catalog>")
    print(f"     export DATABRICKS_SCHEMA=<schema>")
    print(f"  3. Use --config-file flag in deployment scripts:")
    print(f"     python scripts/generate_and_deploy_ldp.py \\")
    print(f"       pipeline.yaml --config-file {args.output} --deploy")

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    main()
