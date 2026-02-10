# LDP Configuration Guide

## Overview

The LDP Generator is now **fully dynamic and configurable**! No more hardcoded values. You can configure it using:

1. **Configuration file** (ldp_config.yaml)
2. **Environment variables**
3. **Command-line arguments**

Configuration priority: **CLI args > Config file > Environment variables > Defaults**

## Quick Start

### Option 1: Auto-Detect Configuration

Let the system detect your settings:

```bash
python scripts/init_ldp_config.py --detect
```

This creates `ldp_config.yaml` with auto-detected values:
- Profile from `~/.databrickscfg`
- Workspace user from Databricks CLI
- Default catalog and schema from environment

### Option 2: Create Template

Create a template config file to edit manually:

```bash
python scripts/init_ldp_config.py
# Edit ldp_config.yaml with your settings
```

### Option 3: Use Environment Variables

```bash
export DATABRICKS_PROFILE=my-profile
export DATABRICKS_CATALOG=my_catalog
export DATABRICKS_SCHEMA=my_schema
export DATABRICKS_USER=my.email@company.com

# No config file needed!
python scripts/generate_and_deploy_ldp.py pipeline.yaml --deploy
```

## Configuration File Format

**ldp_config.yaml:**

```yaml
ldp:
  # Databricks connection
  profile: my-databricks-profile
  workspace_host: null  # Optional, auto-detected from profile

  # Unity Catalog settings
  catalog: my_catalog
  schema: my_schema

  # Workspace settings
  workspace_user: my.email@company.com  # Optional, auto-detected
  workspace_base_path: /Workspace/Users/my.email@company.com/ldp_pipelines

  # Pipeline settings
  serverless: true
  channel: CURRENT
  edition: ADVANCED
  development: true
```

## Environment Variables

All settings can be configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRICKS_PROFILE` | Databricks CLI profile name | `DEFAULT` |
| `DATABRICKS_HOST` | Databricks workspace URL | Auto-detected |
| `DATABRICKS_CATALOG` | Unity Catalog name | `main` |
| `DATABRICKS_SCHEMA` | Schema name | `default` |
| `DATABRICKS_USER` | Workspace username | Auto-detected |
| `DATABRICKS_WORKSPACE_PATH` | Base path for pipeline files | Auto-detected |
| `DATABRICKS_SERVERLESS` | Use serverless compute | `true` |
| `DATABRICKS_CHANNEL` | Runtime channel | `CURRENT` |
| `DATABRICKS_EDITION` | Pipeline edition | `ADVANCED` |
| `DATABRICKS_DEV_MODE` | Development mode | `true` |

## Command-Line Arguments

Override any setting with CLI arguments:

```bash
python scripts/generate_and_deploy_ldp.py pipeline.yaml \
  --config-file ldp_config.yaml \
  --profile my-profile \
  --catalog my_catalog \
  --schema my_schema \
  --deploy
```

## Usage Examples

### Example 1: Using Config File

```bash
# Create config
python scripts/init_ldp_config.py --detect --output my_config.yaml

# Deploy pipeline
python scripts/generate_and_deploy_ldp.py \
  configs/databricks/user_analytics_databricks.yaml \
  --config-file my_config.yaml \
  --deploy
```

### Example 2: Using Environment Variables

```bash
# Set environment
export DATABRICKS_PROFILE=production
export DATABRICKS_CATALOG=analytics_prod
export DATABRICKS_SCHEMA=pipelines

# Deploy (no config file needed!)
python scripts/generate_and_deploy_ldp.py \
  configs/databricks/user_analytics_databricks.yaml \
  --deploy
```

### Example 3: Mixed Configuration

```bash
# Base config from file
# Override catalog and schema via CLI
python scripts/generate_and_deploy_ldp.py \
  configs/databricks/user_analytics_databricks.yaml \
  --config-file ldp_config.yaml \
  --catalog my_test_catalog \
  --schema my_test_schema \
  --deploy
```

### Example 4: Create Job

```bash
python scripts/create_ldp_job.py <PIPELINE_ID> \
  --config-file ldp_config.yaml \
  --job-name "My Pipeline Job" \
  --schedule "0 0 2 * * ?"
```

## Auto-Detection Features

### Workspace User Detection

The system automatically detects your Databricks username:

1. From `workspace_user` in config file
2. From `DATABRICKS_USER` environment variable
3. From `databricks current-user me` CLI command
4. From `$USER` environment variable (fallback)

### Workspace Path Generation

Workspace paths are automatically generated:

```
/Workspace/Users/{detected_user}/ldp_pipelines/{pipeline_name}.sql
```

You can override with:
- `workspace_base_path` in config file
- `DATABRICKS_WORKSPACE_PATH` environment variable

### Profile Detection

Databricks profile is detected from:

1. `--profile` CLI argument
2. `profile` in config file
3. `DATABRICKS_PROFILE` environment variable
4. `DEFAULT` profile (fallback)

## Multi-Environment Setup

### Development Environment

**ldp_config_dev.yaml:**
```yaml
ldp:
  profile: dev-profile
  catalog: dev_catalog
  schema: dev_schema
  development: true
```

```bash
python scripts/generate_and_deploy_ldp.py pipeline.yaml \
  --config-file ldp_config_dev.yaml --deploy
```

### Production Environment

**ldp_config_prod.yaml:**
```yaml
ldp:
  profile: prod-profile
  catalog: prod_catalog
  schema: prod_schema
  development: false  # Production mode
  serverless: true
```

```bash
python scripts/generate_and_deploy_ldp.py pipeline.yaml \
  --config-file ldp_config_prod.yaml --deploy --production
```

## Configuration Validation

The system validates configuration at runtime:

```python
from spark_pipeline.ldp_generator import load_config

# Load and validate
config = load_config(config_file="ldp_config.yaml")

# Access settings
print(f"Profile: {config.profile}")
print(f"Target: {config.catalog}.{config.schema}")
print(f"Workspace path: {config.get_workspace_base_path()}")
```

## Programmatic Usage

Use the configuration system in your Python code:

```python
from spark_pipeline.ldp_generator import LDPConfig, LDPDeployer

# Load from file
config = LDPConfig.from_file("ldp_config.yaml")

# Or from environment
config = LDPConfig.from_env(profile="my-profile")

# Use with deployer
deployer = LDPDeployer(config=config)

# Deploy pipeline
result = deployer.create_pipeline(
    name="my_pipeline",
    sql_file_path="pipeline.sql",
    catalog=config.catalog,
    schema=config.schema,
)
```

## Troubleshooting

### "Profile not found"

```bash
# List available profiles
databricks auth profiles

# Use specific profile
python scripts/init_ldp_config.py --profile <profile-name> --detect
```

### "Workspace user not detected"

```bash
# Set explicitly
export DATABRICKS_USER=your.email@company.com

# Or in config file
echo "  workspace_user: your.email@company.com" >> ldp_config.yaml
```

### "Catalog does not exist"

```bash
# Verify catalog exists
databricks catalogs list --profile <profile>

# Create if needed
databricks catalogs create --name my_catalog --profile <profile>
```

## Best Practices

1. **Use config files for team projects**
   - Commit `ldp_config.yaml` (without secrets) to git
   - Team members can override with local environment variables

2. **Use environment variables for CI/CD**
   - Set `DATABRICKS_*` variables in CI pipeline
   - No config file needed in automation

3. **Use separate configs for environments**
   - `ldp_config_dev.yaml`
   - `ldp_config_staging.yaml`
   - `ldp_config_prod.yaml`

4. **Never hardcode credentials**
   - Use Databricks CLI authentication
   - Rely on auto-detection from profiles

5. **Validate before deployment**
   - Test with `--deploy` on development first
   - Use `--production` flag for prod deployments

## Migration from Hardcoded Version

If you're upgrading from the hardcoded version:

```bash
# 1. Initialize config with your settings
python scripts/init_ldp_config.py --detect \
  --profile spark-pipeline-testing \
  --catalog nsangana_catalog \
  --schema spark_pipeline_test

# 2. All scripts now work with your config
python scripts/generate_and_deploy_ldp.py pipeline.yaml \
  --config-file ldp_config.yaml --deploy

# 3. Or use environment variables instead
export DATABRICKS_PROFILE=spark-pipeline-testing
export DATABRICKS_CATALOG=nsangana_catalog
export DATABRICKS_SCHEMA=spark_pipeline_test

python scripts/generate_and_deploy_ldp.py pipeline.yaml --deploy
```

## Summary

✅ **No more hardcoded values!**
✅ **Works for any Databricks workspace**
✅ **Flexible configuration options**
✅ **Auto-detection of settings**
✅ **Multi-environment support**
✅ **Team-friendly**
✅ **CI/CD ready**

The LDP Generator is now a **universal tool** that works for any user, workspace, and environment!
