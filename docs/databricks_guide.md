# Databricks Integration Guide

Complete guide for using the Spark Pipeline Framework with Azure Databricks.

## Quick Start

### 1. Install Databricks Connect

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies (Databricks Connect, not PySpark)
pip install -r requirements-databricks-dev.txt
pip install -e .
```

**Important**: Do NOT install `pyspark` alongside `databricks-connect`. They conflict with each other.

### 2. Authenticate with Databricks

```bash
# Authenticate using OAuth
databricks auth login --host https://adb-984752964297111.11.azuredatabricks.net --profile spark-pipeline-testing

# Verify authentication
databricks auth profiles
```

### 3. Run Tests

```bash
# Set profile
export DATABRICKS_CONFIG_PROFILE=spark-pipeline-testing

# Run all Databricks tests
pytest tests/test_databricks_integration.py -v

# Run specific test
pytest tests/test_databricks_integration.py::test_databricks_connection -v
```

## Configuration

### Unity Catalog Tables

When writing to Unity Catalog tables, use the full table name in your pipeline configuration:

```yaml
target:
  type: "delta"
  path: "catalog_name.schema_name.table_name"  # Unity Catalog format
  mode: "overwrite"
  partition_by: ["date"]
```

**The framework automatically detects Unity Catalog tables** and uses the appropriate API (`saveAsTable()` instead of `save()`).

### File Paths (DBFS, S3, ADLS)

For file-based Delta tables, use standard paths:

```yaml
target:
  type: "delta"
  path: "dbfs:/mnt/data/my_table"  # DBFS
  # OR
  path: "s3://bucket/path/my_table"  # S3
  # OR
  path: "abfss://container@storage.dfs.core.windows.net/path/my_table"  # ADLS
  mode: "overwrite"
```

## Example Pipeline for Databricks

### Configuration

`configs/databricks_user_analytics.yaml`:

```yaml
pipeline:
  name: "databricks_user_analytics"
  description: "User analytics pipeline on Databricks"

# Spark config is optional - Databricks manages this
spark:
  app_name: "User Analytics on Databricks"

sources:
  - name: "events"
    type: "delta"
    path: "nsangana_catalog.raw.user_events"  # Unity Catalog source

  - name: "user_profiles"
    type: "delta"
    path: "nsangana_catalog.raw.user_profiles"

transformations:
  - name: "calculate_daily_metrics"
    type: "python"
    inputs: ["events"]
    output: "daily_metrics"
    function: "transformations.user_analytics.calculate_daily_metrics"

  - name: "enrich_with_profiles"
    type: "python"
    inputs: ["daily_metrics", "user_profiles"]
    output: "enriched_metrics"
    function: "transformations.user_analytics.enrich_with_profiles"

validation:
  enabled: true
  fail_on_error: true
  rules:
    - name: "check_user_id_not_null"
      type: "null_check"
      column: "user_id"
      threshold: 0.0

    - name: "check_positive_events"
      type: "range_check"
      column: "total_events"
      min_value: 1

    - name: "check_minimum_records"
      type: "row_count"
      min_count: 1

target:
  type: "delta"
  path: "nsangana_catalog.analytics.user_daily_metrics"  # Unity Catalog target
  mode: "overwrite"
  partition_by: ["date"]
  optimize:
    enabled: true
    zorder_by: ["user_id"]
```

### Running the Pipeline

```python
from spark_pipeline.core.engine import PipelineEngine

# Initialize engine with Databricks config
engine = PipelineEngine("configs/databricks_user_analytics.yaml")

# Execute pipeline
result = engine.execute()

if result.status == "success":
    print(f"✓ Pipeline completed successfully")
    print(f"  Rows written: {result.metrics['rows_written']}")
    print(f"  Validation passed: {result.validation_passed}")
else:
    print(f"✗ Pipeline failed: {result.error}")
```

Or using the CLI:

```bash
python scripts/run_pipeline.py configs/databricks_user_analytics.yaml
```

## Using Databricks Connect in Code

### Create Spark Session

```python
from databricks.connect import DatabricksSession

# Create session using profile
spark = DatabricksSession.builder.profile("spark-pipeline-testing").getOrCreate()

# Or specify connection details explicitly
spark = (
    DatabricksSession.builder
    .host("https://adb-984752964297111.11.azuredatabricks.net")
    .token("dapi...")
    .serverless(True)
    .getOrCreate()
)
```

### Write to Unity Catalog

```python
from pyspark.sql import functions as F

# Create sample data
df = spark.createDataFrame([
    (1, "Alice", "2024-01-01"),
    (2, "Bob", "2024-01-01"),
], ["id", "name", "date"])

# Write to Unity Catalog table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("nsangana_catalog.spark_pipeline_test.my_table")

# Read from Unity Catalog table
result = spark.sql("SELECT * FROM nsangana_catalog.spark_pipeline_test.my_table")
result.show()
```

## Authentication Methods

### 1. OAuth (Recommended)

```bash
databricks auth login --host <workspace-url> --profile <profile-name>
```

This opens a browser for authentication and stores credentials securely.

### 2. Personal Access Token (PAT)

Generate a PAT from Databricks workspace:
1. Go to User Settings > Developer > Access Tokens
2. Generate New Token
3. Copy the token

Update `~/.databrickscfg`:

```ini
[spark-pipeline-testing]
host                  = https://adb-984752964297111.11.azuredatabricks.net
token                 = dapi...your-token-here...
serverless_compute_id = auto
```

### 3. Azure CLI (for Azure Databricks)

```bash
# Authenticate with Azure
az login

# Use Azure CLI authentication in config
[spark-pipeline-testing]
host      = https://adb-984752964297111.11.azuredatabricks.net
auth_type = azure-cli
```

## Serverless vs Cluster Compute

### Serverless Compute (Recommended)

```ini
[profile-name]
host                  = <workspace-url>
token                 = <token>
serverless_compute_id = auto
```

**Advantages**:
- No cluster management
- Fast startup
- Auto-scaling
- Cost-effective for development

### Cluster Compute

```ini
[profile-name]
host       = <workspace-url>
token      = <token>
cluster_id = <cluster-id>
```

**Use when**:
- You need specific cluster configurations
- Using custom libraries
- Running long-running jobs

## Testing on Databricks

### Unit Tests

Unit tests don't require Databricks - they test logic in isolation:

```bash
# These work with or without Databricks
pytest tests/unit/test_config_parser.py -v
```

### Integration Tests

Integration tests use actual Databricks compute:

```bash
# Requires Databricks authentication
export DATABRICKS_CONFIG_PROFILE=spark-pipeline-testing
pytest tests/test_databricks_integration.py -v
```

### CI/CD Integration

For automated testing in CI/CD pipelines:

```yaml
# .github/workflows/test.yml
name: Databricks Tests

on: [push]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2

      - name: Install dependencies
        run: |
          pip install -r requirements-databricks-dev.txt
          pip install -e .

      - name: Setup Databricks authentication
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          mkdir -p ~/.databrickscfg
          echo "[DEFAULT]" > ~/.databrickscfg
          echo "host = ${DATABRICKS_HOST}" >> ~/.databrickscfg
          echo "token = ${DATABRICKS_TOKEN}" >> ~/.databrickscfg
          echo "serverless_compute_id = auto" >> ~/.databrickscfg

      - name: Run tests
        run: pytest tests/test_databricks_integration.py -v
```

## Best Practices

### 1. Use Unity Catalog

Always write to Unity Catalog tables for:
- Better governance
- Fine-grained access control
- Data lineage
- Easier data discovery

### 2. Enable Data Quality Validation

Always enable validation in production:

```yaml
validation:
  enabled: true
  fail_on_error: true
  rules:
    - name: "critical_check"
      type: "null_check"
      column: "primary_key"
      threshold: 0.0
```

### 3. Partition Large Tables

```yaml
target:
  partition_by: ["year", "month", "day"]
```

### 4. Optimize Delta Tables

```yaml
target:
  optimize:
    enabled: true
    zorder_by: ["frequently_filtered_column"]
  vacuum:
    enabled: true
    retention_hours: 168  # 7 days
```

### 5. Use Serverless for Development

Serverless compute is faster and cheaper for development and testing.

### 6. Separate Catalogs/Schemas

Use different catalogs or schemas for:
- Development: `nsangana_catalog.dev.table`
- Staging: `nsangana_catalog.staging.table`
- Production: `nsangana_catalog.prod.table`

## Troubleshooting

### Error: "pyspark and databricks-connect cannot be installed at the same time"

**Solution**:
```bash
pip uninstall -y pyspark databricks-connect
pip install databricks-connect
```

### Error: "Invalid access token"

**Solution**:
```bash
# Re-authenticate
databricks auth login --host <workspace-url> --profile <profile-name>

# Or generate a new PAT and update ~/.databrickscfg
```

### Error: "Table not found"

**Solution**:
- Verify the catalog and schema exist
- Check permissions on the catalog/schema
- Use full three-part names: `catalog.schema.table`

### Error: "Path must be absolute"

**Solution**:
- This error occurs when trying to write to Unity Catalog with `save()` instead of `saveAsTable()`
- The framework automatically handles this, but if you see this error, ensure your path is correctly formatted:
  - Unity Catalog: `catalog.schema.table` (no slashes)
  - File path: `dbfs:/path/to/table` or `s3://bucket/path`

## Performance Tips

### 1. Use Broadcast Joins for Small Tables

```python
from pyspark.sql.functions import broadcast

df_large.join(broadcast(df_small), on="key")
```

### 2. Cache Expensive Computations

```python
expensive_df = some_complex_operation(df)
expensive_df.cache()
```

### 3. Optimize Shuffle Partitions

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: "200"  # Adjust based on data size
```

### 4. Use Delta Lake Features

- Enable auto-optimize
- Use Z-ordering
- Enable auto-vacuum
- Use liquid clustering (Databricks feature)

## Next Steps

1. **Try the Example**: Run the Databricks tests to verify your setup
2. **Create Your Pipeline**: Use the examples as templates
3. **Deploy to Production**: Set up CI/CD with Databricks authentication
4. **Monitor**: Use Databricks job monitoring and Spark UI

## Resources

- [Databricks Connect Documentation](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
