"""Configuration management for LDP Generator."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class LDPConfig:
    """Configuration for LDP pipeline generation and deployment."""

    # Databricks settings
    profile: str
    workspace_host: Optional[str] = None

    # Unity Catalog settings
    catalog: str = "main"
    schema: str = "default"

    # Workspace settings
    workspace_user: Optional[str] = None
    workspace_base_path: Optional[str] = None

    # Pipeline settings
    serverless: bool = True
    channel: str = "CURRENT"
    edition: str = "ADVANCED"
    development: bool = True

    @classmethod
    def from_file(cls, config_path: str) -> "LDPConfig":
        """Load configuration from YAML file.

        Args:
            config_path: Path to config file

        Returns:
            LDPConfig instance
        """
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        return cls(**data.get("ldp", {}))

    @classmethod
    def from_env(cls, profile: Optional[str] = None) -> "LDPConfig":
        """Load configuration from environment variables.

        Args:
            profile: Databricks profile name (or use DATABRICKS_PROFILE env var)

        Returns:
            LDPConfig instance
        """
        return cls(
            profile=profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT"),
            workspace_host=os.getenv("DATABRICKS_HOST"),
            catalog=os.getenv("DATABRICKS_CATALOG", "main"),
            schema=os.getenv("DATABRICKS_SCHEMA", "default"),
            workspace_user=os.getenv("DATABRICKS_USER"),
            workspace_base_path=os.getenv("DATABRICKS_WORKSPACE_PATH"),
            serverless=os.getenv("DATABRICKS_SERVERLESS", "true").lower() == "true",
            channel=os.getenv("DATABRICKS_CHANNEL", "CURRENT"),
            edition=os.getenv("DATABRICKS_EDITION", "ADVANCED"),
            development=os.getenv("DATABRICKS_DEV_MODE", "true").lower() == "true",
        )

    def get_workspace_user(self) -> str:
        """Get workspace username.

        Returns:
            Workspace username
        """
        if self.workspace_user:
            return self.workspace_user

        # Try to detect from Databricks config
        try:
            import subprocess
            result = subprocess.run(
                ["databricks", "current-user", "me", "--profile", self.profile],
                capture_output=True,
                text=True,
                check=True,
            )
            import json
            user_info = json.loads(result.stdout)
            return user_info.get("userName", "default-user")
        except Exception:
            # Fallback to environment user
            return os.getenv("USER", "default-user")

    def get_workspace_base_path(self) -> str:
        """Get workspace base path for pipeline files.

        Returns:
            Workspace base path
        """
        if self.workspace_base_path:
            return self.workspace_base_path

        user = self.get_workspace_user()
        return f"/Workspace/Users/{user}/ldp_pipelines"

    def get_storage_location(self, schema: Optional[str] = None) -> str:
        """Get default storage location for pipeline.

        Args:
            schema: Schema name (uses default if not provided)

        Returns:
            Storage location path
        """
        schema = schema or self.schema
        return f"/Volumes/{self.catalog}/{schema}/_pipeline_storage"


def load_config(
    config_file: Optional[str] = None,
    profile: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> LDPConfig:
    """Load configuration from file or environment.

    Priority order:
    1. Explicit parameters
    2. Config file (if provided)
    3. Environment variables
    4. Defaults

    Args:
        config_file: Path to config file
        profile: Databricks profile
        catalog: Unity Catalog name
        schema: Schema name

    Returns:
        LDPConfig instance
    """
    # Start with environment defaults
    if config_file and Path(config_file).exists():
        config = LDPConfig.from_file(config_file)
    else:
        config = LDPConfig.from_env(profile)

    # Override with explicit parameters
    if profile:
        config.profile = profile
    if catalog:
        config.catalog = catalog
    if schema:
        config.schema = schema

    return config


def create_default_config_file(output_path: str = "ldp_config.yaml"):
    """Create a default configuration file.

    Args:
        output_path: Where to save the config file
    """
    default_config = {
        "ldp": {
            "profile": "DEFAULT",
            "catalog": "main",
            "schema": "default",
            "workspace_user": None,  # Auto-detected
            "workspace_base_path": None,  # Auto-detected
            "serverless": True,
            "channel": "CURRENT",
            "edition": "ADVANCED",
            "development": True,
        }
    }

    with open(output_path, "w") as f:
        yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)

    print(f"✓ Created default config: {output_path}")
    print(f"\nEdit this file to customize your LDP settings.")
    print(f"You can also use environment variables:")
    print(f"  - DATABRICKS_PROFILE")
    print(f"  - DATABRICKS_CATALOG")
    print(f"  - DATABRICKS_SCHEMA")
    print(f"  - DATABRICKS_USER")
    print(f"  - DATABRICKS_WORKSPACE_PATH")
