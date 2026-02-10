# Transformation Guide

Learn how to write and use data transformations in the pipeline framework.

## Transformation Types

The framework supports three types of transformations:

1. **SQL Transformations** - SQL queries on registered DataFrames
2. **Python Transformations** - Custom Python functions
3. **Built-in Transformations** - Pre-built operations (filter, join, aggregate)

## SQL Transformations

SQL transformations execute SQL queries on DataFrames registered as temporary views.

### Basic Example

```yaml
transformations:
  - name: "filter_adults"
    type: "sql"
    inputs: ["users"]
    output: "adults"
    sql: "SELECT * FROM users WHERE age >= 18"
```

### Multi-line SQL

```yaml
transformations:
  - name: "complex_query"
    type: "sql"
    inputs: ["orders", "customers"]
    output: "order_summary"
    sql: |
      SELECT
        c.customer_id,
        c.name,
        COUNT(o.order_id) as total_orders,
        SUM(o.amount) as total_revenue
      FROM customers c
      JOIN orders o ON c.customer_id = o.customer_id
      WHERE o.status = 'completed'
      GROUP BY c.customer_id, c.name
```

### Using Multiple Inputs

All input DataFrames are available as SQL tables:

```yaml
transformations:
  - name: "join_tables"
    type: "sql"
    inputs: ["events", "users", "products"]
    output: "enriched_events"
    sql: |
      SELECT
        e.event_id,
        u.name as user_name,
        p.product_name,
        e.timestamp
      FROM events e
      JOIN users u ON e.user_id = u.user_id
      JOIN products p ON e.product_id = p.product_id
```

## Python Transformations

Write custom transformation logic in Python functions.

### Basic Python Function

Create `transformations/my_transforms.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_timestamp_features(df: DataFrame) -> DataFrame:
    """Add date/time features from timestamp column."""
    return (
        df
        .withColumn("date", F.to_date(F.col("timestamp")))
        .withColumn("hour", F.hour(F.col("timestamp")))
        .withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
    )
```

Use in config:

```yaml
transformations:
  - name: "add_features"
    type: "python"
    inputs: ["events"]
    output: "events_with_features"
    function: "transformations.my_transforms.add_timestamp_features"
```

### Function with Multiple Inputs

```python
def join_and_aggregate(events_df: DataFrame, users_df: DataFrame) -> DataFrame:
    """Join events with users and calculate metrics."""
    joined = events_df.join(users_df, on="user_id", how="inner")

    aggregated = (
        joined
        .groupBy("user_id", "segment")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("unique_events"),
        )
    )

    return aggregated
```

Config:

```yaml
transformations:
  - name: "join_and_aggregate"
    type: "python"
    inputs: ["events", "users"]  # Order matters!
    output: "user_metrics"
    function: "transformations.my_transforms.join_and_aggregate"
```

### Function with Parameters

```python
def filter_by_threshold(
    df: DataFrame,
    column: str,
    threshold: float,
    operator: str = "greater"
) -> DataFrame:
    """Filter DataFrame by threshold."""
    if operator == "greater":
        return df.filter(F.col(column) > threshold)
    elif operator == "less":
        return df.filter(F.col(column) < threshold)
    else:
        return df.filter(F.col(column) == threshold)
```

Config with params:

```yaml
transformations:
  - name: "filter_high_value"
    type: "python"
    inputs: ["transactions"]
    output: "high_value_transactions"
    function: "transformations.my_transforms.filter_by_threshold"
    params:
      column: "amount"
      threshold: 1000.0
      operator: "greater"
```

## Built-in Transformations

Pre-built operations for common use cases.

### Filter

Filter rows based on a condition:

```yaml
transformations:
  - name: "filter_active"
    type: "filter"
    inputs: ["users"]
    output: "active_users"
    condition: "status = 'active' AND last_login > '2024-01-01'"
```

### Join

Join two DataFrames:

```yaml
transformations:
  - name: "join_users_orders"
    type: "join"
    inputs: ["users", "orders"]
    output: "users_with_orders"
    on: "user_id"
    how: "left"  # inner, left, right, outer
```

Multiple join keys:

```yaml
transformations:
  - name: "complex_join"
    type: "join"
    inputs: ["table1", "table2"]
    output: "joined"
    on: ["key1", "key2"]
    how: "inner"
```

### Aggregate

Group and aggregate data:

```yaml
transformations:
  - name: "daily_aggregates"
    type: "aggregate"
    inputs: ["events"]
    output: "daily_metrics"
    group_by: ["user_id", "date"]
    aggregations:
      revenue: "sum"
      events: "count"
      score: "avg"
      max_value: "max"
      min_value: "min"
```

## Advanced Patterns

### Chaining Transformations

Build complex pipelines by chaining transformations:

```yaml
transformations:
  # Step 1: Filter
  - name: "filter_valid_events"
    type: "filter"
    inputs: ["raw_events"]
    output: "valid_events"
    condition: "event_type IS NOT NULL"

  # Step 2: Add features
  - name: "add_features"
    type: "python"
    inputs: ["valid_events"]
    output: "events_with_features"
    function: "transformations.features.add_timestamp_features"

  # Step 3: Join with users
  - name: "enrich_with_users"
    type: "join"
    inputs: ["events_with_features", "users"]
    output: "enriched_events"
    on: "user_id"
    how: "left"

  # Step 4: Aggregate
  - name: "calculate_metrics"
    type: "aggregate"
    inputs: ["enriched_events"]
    output: "daily_metrics"
    group_by: ["user_id", "date"]
    aggregations:
      event_count: "count"
```

### Window Functions

Use window functions in SQL transformations:

```yaml
transformations:
  - name: "add_row_number"
    type: "sql"
    inputs: ["events"]
    output: "ranked_events"
    sql: |
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY timestamp DESC
        ) as event_rank
      FROM events
```

Or in Python:

```python
from pyspark.sql import Window

def add_ranking(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("timestamp"))
    return df.withColumn("event_rank", F.row_number().over(window_spec))
```

### Complex Aggregations

```python
def calculate_complex_metrics(df: DataFrame) -> DataFrame:
    """Calculate complex business metrics."""
    return (
        df
        .groupBy("user_id", "date")
        .agg(
            # Basic counts
            F.count("*").alias("total_events"),
            F.countDistinct("session_id").alias("total_sessions"),

            # Conditional counts
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))
                .alias("purchases"),

            # Array aggregation
            F.collect_list("product_id").alias("viewed_products"),

            # Statistical functions
            F.avg("session_duration").alias("avg_session_duration"),
            F.stddev("session_duration").alias("stddev_session_duration"),

            # Percentiles
            F.expr("percentile_approx(revenue, 0.5)").alias("median_revenue"),
        )
    )
```

## Best Practices

### 1. Function Organization

Organize transformation functions by domain:

```
transformations/
├── __init__.py
├── features.py          # Feature engineering
├── aggregations.py      # Aggregation functions
├── enrichment.py        # Data enrichment
└── user_analytics.py    # Domain-specific
```

### 2. Type Hints

Always use type hints for clarity:

```python
from pyspark.sql import DataFrame

def my_transform(df: DataFrame, param: int) -> DataFrame:
    """Clear docstring explaining the transformation."""
    return df
```

### 3. Null Handling

Always handle null values:

```python
def safe_division(df: DataFrame) -> DataFrame:
    """Calculate conversion rate with null safety."""
    return df.withColumn(
        "conversion_rate",
        F.when(F.col("visits") > 0, F.col("purchases") / F.col("visits"))
        .otherwise(0.0)
    )
```

### 4. Testing Transformations

Write unit tests for transformation functions:

```python
# tests/unit/test_my_transforms.py

def test_add_timestamp_features(spark_session):
    from transformations.my_transforms import add_timestamp_features

    input_df = spark_session.createDataFrame([
        (1, "2024-01-15 10:30:00"),
    ], ["id", "timestamp"])

    result = add_timestamp_features(input_df)

    assert "date" in result.columns
    assert "hour" in result.columns
    assert result.count() == 1
```

### 5. Performance Optimization

#### Cache Intermediate Results

For expensive transformations used multiple times:

```python
def expensive_transformation(df: DataFrame) -> DataFrame:
    result = df.filter(...).join(...).groupBy(...)
    return result.cache()  # Cache the result
```

#### Broadcast Small DataFrames

```python
from pyspark.sql.functions import broadcast

def join_with_small_table(large_df: DataFrame, small_df: DataFrame) -> DataFrame:
    return large_df.join(broadcast(small_df), on="key")
```

#### Repartition When Needed

```python
def repartition_before_write(df: DataFrame) -> DataFrame:
    # Repartition to reduce small files
    return df.repartition(10, "partition_key")
```

### 6. Documentation

Document complex transformations:

```python
def calculate_user_segments(df: DataFrame) -> DataFrame:
    """Classify users into segments based on behavior.

    Segments:
    - VIP: >10 purchases AND >$1000 revenue
    - Active: >5 purchases OR >$500 revenue
    - Casual: <5 purchases AND <$500 revenue

    Args:
        df: DataFrame with user_id, purchases, revenue columns

    Returns:
        DataFrame with additional 'segment' column
    """
    return (
        df
        .withColumn(
            "segment",
            F.when(
                (F.col("purchases") > 10) & (F.col("revenue") > 1000),
                "VIP"
            )
            .when(
                (F.col("purchases") > 5) | (F.col("revenue") > 500),
                "Active"
            )
            .otherwise("Casual")
        )
    )
```

## Common Patterns

### Date Range Filtering

```python
def filter_date_range(
    df: DataFrame,
    start_date: str,
    end_date: str
) -> DataFrame:
    return df.filter(
        (F.col("date") >= F.lit(start_date)) &
        (F.col("date") <= F.lit(end_date))
    )
```

### Deduplication

```python
def deduplicate(df: DataFrame, key_columns: list) -> DataFrame:
    """Remove duplicates keeping the latest record."""
    window_spec = Window.partitionBy(*key_columns).orderBy(F.desc("updated_at"))

    return (
        df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
```

### Pivot Operations

```python
def pivot_metrics(df: DataFrame) -> DataFrame:
    """Pivot event types into columns."""
    return (
        df
        .groupBy("user_id", "date")
        .pivot("event_type", ["page_view", "click", "purchase"])
        .agg(F.count("*"))
    )
```

## Debugging Tips

### 1. Add Logging

```python
from spark_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)

def my_transform(df: DataFrame) -> DataFrame:
    logger.info(f"Input rows: {df.count()}")

    result = df.filter(...)

    logger.info(f"Output rows: {result.count()}")
    return result
```

### 2. Sample Data for Testing

```python
def my_transform(df: DataFrame) -> DataFrame:
    # For development, sample the data
    if df.count() > 1000000:
        df = df.sample(0.1)

    # Your transformation logic
    return df
```

### 3. Show Intermediate Results

```python
def debug_transform(df: DataFrame) -> DataFrame:
    df.show(5)  # Show sample during development
    df.printSchema()  # Verify schema

    # Your transformation
    result = df.filter(...)

    result.show(5)
    return result
```

## Next Steps

- Review [Configuration Guide](configuration_guide.md) for YAML syntax
- Check [Testing Guide](testing_guide.md) for testing strategies
- Explore [Examples](../examples/user_analytics/README.md) for real implementations
