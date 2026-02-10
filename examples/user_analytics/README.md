# User Analytics Example

This example demonstrates a complete user analytics pipeline that processes user events and generates daily engagement metrics.

## Pipeline Overview

The pipeline performs the following steps:

1. **Read Data Sources**
   - User events (page views, clicks, purchases, etc.)
   - User profiles (name, segment, signup date)

2. **Transform Data**
   - Calculate daily metrics per user (total events, page views, clicks, purchases)
   - Enrich with user profile information
   - Filter to active users only (minimum 3 events)

3. **Validate Data Quality**
   - Check for null user IDs
   - Verify total events are positive
   - Validate engagement score range
   - Ensure minimum record count

4. **Write to Delta Lake**
   - Write to partitioned Delta table (partitioned by date)
   - Optimize with Z-ordering on user_id
   - Only write if ALL validations pass

## Setup

### 1. Generate Sample Data

```bash
cd examples/user_analytics
python scripts/generate_sample_data.py
```

This creates sample data in `examples/user_analytics/data/`:
- `events/` - 1000 sample user events
- `user_profiles/` - 50 sample user profiles

### 2. Set Environment Variables

```bash
export DATA_PATH=$(pwd)/examples/user_analytics/data
export OUTPUT_PATH=$(pwd)/examples/user_analytics/output
```

## Running the Pipeline

### Using the CLI Script

```bash
python scripts/run_pipeline.py configs/examples/user_analytics.yaml
```

### Using Python

```python
from spark_pipeline.core.engine import PipelineEngine

engine = PipelineEngine("configs/examples/user_analytics.yaml")
result = engine.execute()

print(f"Status: {result.status}")
print(f"Rows written: {result.metrics['rows_written']}")
```

## Expected Output

### Success Case

When all validations pass:

```
✓ Pipeline executed successfully!
  Pipeline: user_analytics
  Rows written: 45
  Validation passed: True
```

### Failure Case

If validation fails (e.g., data quality issues):

```
✗ Pipeline execution failed: Data quality validation failed. Will NOT write to table.
Failed rules: check_user_id_not_null
```

**Note:** When validation fails, NO data is written to the target table.

## Data Quality Rules

The pipeline enforces these validation rules:

1. **No Null User IDs**: `user_id` column must not contain any nulls
2. **Positive Event Counts**: `total_events` must be >= 1
3. **Valid Engagement Scores**: `engagement_score` must be between 0 and 10,000
4. **Minimum Records**: At least 1 record must be present

## Output Schema

The output Delta table contains:

| Column | Type | Description |
|--------|------|-------------|
| user_id | int | User identifier |
| date | date | Date of metrics |
| total_events | long | Total number of events |
| unique_event_types | long | Number of unique event types |
| page_views | long | Number of page views |
| clicks | long | Number of clicks |
| purchases | long | Number of purchases |
| session_duration_minutes | double | Session duration in minutes |
| name | string | User name |
| segment | string | User segment (free/premium/enterprise) |
| signup_date | date | User signup date |
| days_since_signup | int | Days since user signed up |
| engagement_score | int | Calculated engagement score |

## Customization

### Adding New Transformations

Create a new function in `transformations/user_analytics.py`:

```python
def my_custom_transformation(df: DataFrame) -> DataFrame:
    # Your transformation logic
    return df
```

Add to pipeline config:

```yaml
transformations:
  - name: "my_transform"
    type: "python"
    inputs: ["previous_step"]
    output: "my_output"
    function: "transformations.user_analytics.my_custom_transformation"
```

### Adding New Validation Rules

Add to the `validation.rules` section in the YAML config:

```yaml
validation:
  rules:
    - name: "my_custom_rule"
      type: "range_check"
      column: "my_column"
      min_value: 0
      max_value: 100
```

### Changing Write Mode

Modify the `target` section:

```yaml
target:
  mode: "append"  # or "overwrite", "merge"
  partition_by: ["date", "segment"]  # Multiple partition columns
```

## Testing

Run the data quality tests:

```bash
pytest tests/data_quality/test_user_analytics_quality.py -v
```

## Troubleshooting

### Issue: "Environment variable not found"

Make sure to set the required environment variables:
```bash
export DATA_PATH=/path/to/data
export OUTPUT_PATH=/path/to/output
```

### Issue: "Validation failed"

Check the logs to see which validation rule failed:
```
Failed rules: check_user_id_not_null
```

Inspect your data to fix the quality issues before running again.

### Issue: "Module not found"

Make sure you're running from the project root directory and the package is installed:
```bash
pip install -e .
```

## Next Steps

1. Modify the transformation functions to suit your needs
2. Add custom validation rules
3. Integrate with your production Spark cluster
4. Schedule with Airflow or other orchestration tools
5. Add more data sources and enrichment steps
