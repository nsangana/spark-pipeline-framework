# Configuration Guide

Complete reference for pipeline YAML configuration.

## Configuration Structure

```yaml
pipeline:       # Required: Pipeline metadata
spark:          # Optional: Spark configuration
sources:        # Required: Data sources
transformations: # Optional: Data transformations
validation:     # Optional: Data quality validation
target:         # Required: Output destination
```

## Pipeline Section

Basic pipeline metadata.

```yaml
pipeline:
  name: "my_pipeline"           # Required: Unique pipeline name
  description: "Description"     # Optional: Pipeline description
```

## Spark Section

Configure SparkSession settings.

```yaml
spark:
  app_name: "MyApp"              # Optional: Spark application name
  master: "local[*]"             # Optional: Spark master URL
  config:                        # Optional: Additional Spark configs
    spark.sql.shuffle.partitions: "8"
    spark.driver.memory: "4g"
    spark.executor.memory: "4g"
```

### Common Spark Configurations

| Configuration | Description | Default |
|--------------|-------------|---------|
| `spark.sql.shuffle.partitions` | Number of partitions for shuffles | 200 |
| `spark.driver.memory` | Driver memory | 1g |
| `spark.executor.memory` | Executor memory | 1g |
| `spark.sql.adaptive.enabled` | Enable adaptive query execution | false |

## Sources Section

Define input data sources.

```yaml
sources:
  - name: "source_name"          # Required: Unique source name
    type: "parquet"              # Required: parquet, delta, csv, json
    path: "/path/to/data"        # Required: Data path
    options:                     # Optional: Read options
      mergeSchema: "true"
      versionAsOf: "1"
```

### Source Types

#### Parquet

```yaml
- name: "parquet_source"
  type: "parquet"
  path: "/data/parquet"
  options:
    mergeSchema: "true"
```

#### Delta Lake

```yaml
- name: "delta_source"
  type: "delta"
  path: "/data/delta"
  options:
    versionAsOf: "10"          # Read specific version
    # OR
    timestampAsOf: "2024-01-01" # Read at timestamp
```

#### CSV

```yaml
- name: "csv_source"
  type: "csv"
  path: "/data/csv"
  options:
    header: "true"
    inferSchema: "true"
    delimiter: ","
```

#### JSON

```yaml
- name: "json_source"
  type: "json"
  path: "/data/json"
  options:
    multiLine: "true"
```

### Environment Variables

Use environment variables in paths:

```yaml
sources:
  - name: "events"
    type: "parquet"
    path: "${DATA_PATH}/events"
```

With default values:

```yaml
path: "${DATA_PATH:-/default/path}/events"
```

## Transformations Section

Define data transformations.

### SQL Transformations

```yaml
transformations:
  - name: "filter_users"
    type: "sql"
    inputs: ["source_name"]
    output: "filtered_users"
    sql: |
      SELECT *
      FROM source_name
      WHERE age > 18
```

### Python Function Transformations

```yaml
transformations:
  - name: "calculate_metrics"
    type: "python"
    inputs: ["source_name"]
    output: "metrics"
    function: "transformations.my_module.calculate_metrics"
    params:                      # Optional: Parameters
      threshold: 10
      mode: "strict"
```

Python function signature:

```python
def calculate_metrics(df: DataFrame, threshold: int, mode: str) -> DataFrame:
    # Transformation logic
    return result_df
```

### Built-in Transformations

#### Filter

```yaml
- name: "filter_active"
  type: "filter"
  inputs: ["users"]
  output: "active_users"
  condition: "status = 'active' AND last_login > '2024-01-01'"
```

#### Join

```yaml
- name: "join_tables"
  type: "join"
  inputs: ["left_table", "right_table"]
  output: "joined"
  on: "user_id"                # Single column
  # OR
  on: ["user_id", "date"]      # Multiple columns
  how: "inner"                 # inner, left, right, outer
```

#### Aggregate

```yaml
- name: "aggregate_metrics"
  type: "aggregate"
  inputs: ["events"]
  output: "daily_metrics"
  group_by: ["user_id", "date"]
  aggregations:
    revenue: "sum"
    events: "count"
    score: "avg"
```

## Validation Section

Configure data quality validation.

```yaml
validation:
  enabled: true                  # Required: Enable/disable validation
  fail_on_error: true           # Required: Fail pipeline on error
  rules:                        # List of validation rules
    - name: "check_nulls"
      type: "null_check"
      column: "user_id"
      threshold: 0.0
```

### Validation Rules

#### Null Check

Verify null percentage is below threshold.

```yaml
- name: "check_nulls"
  type: "null_check"
  column: "user_id"
  threshold: 5.0               # Max 5% nulls allowed
```

#### Range Check

Verify numeric values are within range.

```yaml
- name: "check_age_range"
  type: "range_check"
  column: "age"
  min_value: 0
  max_value: 120
```

#### Row Count

Verify row count is within bounds.

```yaml
- name: "check_row_count"
  type: "row_count"
  min_count: 100
  max_count: 1000000
```

### Critical: Conditional Writes

If `fail_on_error: true` and ANY validation rule fails:
- Pipeline raises `ValidationError`
- **NO data is written** to the target
- Existing data remains unchanged

## Target Section

Configure output destination.

```yaml
target:
  type: "delta"                 # Required: delta or parquet
  path: "/output/path"          # Required: Output path
  mode: "overwrite"             # Required: append, overwrite, merge
  partition_by: ["date"]        # Optional: Partition columns
  optimize:                     # Optional: Delta optimization
    enabled: true
    zorder_by: ["user_id"]
  vacuum:                       # Optional: Delta vacuum
    enabled: true
    retention_hours: 168
```

### Write Modes

#### Append

Add new data to existing table:

```yaml
target:
  type: "delta"
  path: "/data/table"
  mode: "append"
```

#### Overwrite

Replace all existing data:

```yaml
target:
  type: "delta"
  path: "/data/table"
  mode: "overwrite"
```

#### Merge (Upsert)

Merge based on keys:

```yaml
target:
  type: "delta"
  path: "/data/table"
  mode: "merge"
  merge_keys: ["user_id", "date"]
```

### Partitioning

```yaml
target:
  partition_by: ["year", "month", "day"]
```

### Delta Optimization

#### Optimize (Compact Files)

```yaml
target:
  optimize:
    enabled: true
    zorder_by: ["user_id"]     # Z-order optimization
```

#### Vacuum (Clean Old Files)

```yaml
target:
  vacuum:
    enabled: true
    retention_hours: 168       # Keep 7 days of history
```

## Complete Example

```yaml
pipeline:
  name: "user_analytics"
  description: "Daily user analytics pipeline"

spark:
  app_name: "UserAnalytics"
  master: "local[*]"
  config:
    spark.sql.shuffle.partitions: "4"

sources:
  - name: "events"
    type: "parquet"
    path: "${DATA_PATH}/events"

  - name: "users"
    type: "delta"
    path: "${DATA_PATH}/users"

transformations:
  - name: "filter_active_events"
    type: "sql"
    inputs: ["events"]
    output: "active_events"
    sql: "SELECT * FROM events WHERE status = 'active'"

  - name: "join_with_users"
    type: "join"
    inputs: ["active_events", "users"]
    output: "enriched_events"
    on: "user_id"
    how: "inner"

  - name: "calculate_metrics"
    type: "python"
    inputs: ["enriched_events"]
    output: "daily_metrics"
    function: "transformations.analytics.calculate_daily_metrics"

validation:
  enabled: true
  fail_on_error: true
  rules:
    - name: "check_user_id_nulls"
      type: "null_check"
      column: "user_id"
      threshold: 0.0

    - name: "check_event_count"
      type: "range_check"
      column: "event_count"
      min_value: 1

    - name: "check_minimum_records"
      type: "row_count"
      min_count: 1

target:
  type: "delta"
  path: "${OUTPUT_PATH}/user_daily_metrics"
  mode: "overwrite"
  partition_by: ["date"]
  optimize:
    enabled: true
    zorder_by: ["user_id"]
```

## Best Practices

1. **Use Environment Variables**: Keep paths configurable
2. **Enable Validation**: Always validate data quality
3. **Partition Large Tables**: Use appropriate partition columns
4. **Optimize Delta Tables**: Enable optimization for better query performance
5. **Document Transformations**: Add clear descriptions to complex logic
6. **Version Control**: Keep configs in version control
7. **Test Configs**: Use `validate_config.py` before deployment
