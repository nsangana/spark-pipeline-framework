#!/usr/bin/env python3
"""Helper script to set up Databricks authentication for testing.

This script will guide you through creating a Personal Access Token (PAT)
for Databricks Connect testing.
"""

import os
import sys
from pathlib import Path


def print_instructions():
    """Print instructions for setting up Databricks authentication."""
    print("=" * 70)
    print("Databricks Authentication Setup")
    print("=" * 70)
    print()
    print("To test the Spark Pipeline Framework with Databricks, you need to")
    print("create a Personal Access Token (PAT).")
    print()
    print("Steps to create a PAT:")
    print()
    print("1. Open your Databricks workspace:")
    print("   https://adb-984752964297111.11.azuredatabricks.net")
    print()
    print("2. Click on your username in the top-right corner")
    print()
    print("3. Click 'Settings' or 'User Settings'")
    print()
    print("4. Go to 'Developer' > 'Access Tokens'")
    print()
    print("5. Click 'Generate New Token'")
    print()
    print("6. Enter:")
    print("   - Comment: 'spark-pipeline-testing'")
    print("   - Lifetime: 90 days (or as needed)")
    print()
    print("7. Click 'Generate'")
    print()
    print("8. Copy the token (you won't be able to see it again!)")
    print()
    print("=" * 70)
    print()


def update_config(token: str):
    """Update the Databricks config file with the new token.

    Args:
        token: The Personal Access Token from Databricks
    """
    config_path = Path.home() / ".databrickscfg"

    # Read existing config
    if config_path.exists():
        with open(config_path, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    # Find and update the spark-pipeline-testing profile
    updated = False
    in_target_profile = False
    new_lines = []

    for i, line in enumerate(lines):
        if line.strip() == "[spark-pipeline-testing]":
            in_target_profile = True
            new_lines.append(line)
        elif line.strip().startswith("[") and in_target_profile:
            in_target_profile = False
            new_lines.append(line)
        elif in_target_profile and line.strip().startswith("token"):
            new_lines.append(f"token                 = {token}\n")
            updated = True
        else:
            new_lines.append(line)

    # If profile doesn't exist, add it
    if not updated:
        new_lines.append("\n[spark-pipeline-testing]\n")
        new_lines.append("host                  = https://adb-984752964297111.11.azuredatabricks.net\n")
        new_lines.append(f"token                 = {token}\n")
        new_lines.append("serverless_compute_id = auto\n")

    # Write updated config
    with open(config_path, "w") as f:
        f.writelines(new_lines)

    # Set secure permissions
    os.chmod(config_path, 0o600)

    print(f"✓ Updated {config_path}")
    print()


def main():
    """Main function."""
    print_instructions()

    # Get token from user
    print("Enter your Personal Access Token:")
    token = input("> ").strip()

    if not token:
        print("Error: Token cannot be empty")
        sys.exit(1)

    if not token.startswith("dapi"):
        print("Warning: Token should start with 'dapi'. Are you sure this is correct?")
        confirm = input("Continue anyway? (y/n): ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(1)

    # Update config
    update_config(token)

    print("=" * 70)
    print("Setup Complete!")
    print("=" * 70)
    print()
    print("You can now run the Databricks tests:")
    print()
    print("  pytest tests/test_databricks_integration.py -v")
    print()
    print("Or run specific tests:")
    print()
    print("  pytest tests/test_databricks_integration.py::test_databricks_connection -v")
    print()


if __name__ == "__main__":
    main()
