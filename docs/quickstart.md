# Quick Start Guide

Get up and running with the Spark Pipeline Framework in minutes.

## Installation

### Prerequisites

- Python 3.9+
- Java 8 or 11 (for Spark)
- Sufficient memory for Spark (at least 4GB recommended)

### Install Dependencies

```bash
# Clone the repository
git clone <your-repo-url>
cd claude_first_project

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make install-dev

# Or manually:
pip install -r requirements-dev.txt
pip install -e .
```

## Your First Pipeline

### 1. Create a Configuration File

Create `my_pipeline.yaml`:

```yaml
pipeline:
  name: "my_first_pipeline"

sources:
  - name: "input_data"
    type: "parquet"
    path: "/path/to/input"

validation:
  enabled: true
  fail_on_error: true
  rules:
    - name: "check_nulls"
      type: "null_check"
      column: "id"
      threshold: 0.0

target:
  type: "delta"
  path: "/path/to/output"
  mode: "overwrite"
```

### 2. Run the Pipeline

```bash
python scripts/run_pipeline.py my_pipeline.yaml
```

### 3. View Results

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ViewResults").getOrCreate()
df = spark.read.format("delta").load("/path/to/output")
df.show()
```

## Running the Example

The framework includes a complete user analytics example:

### 1. Generate Sample Data

```bash
make generate-data
```

### 2. Run the Example Pipeline

```bash
make run-example
```

### 3. Inspect Output

```bash
# The output will be in examples/user_analytics/output/user_daily_metrics
```

## Next Steps

- [Configuration Guide](configuration_guide.md) - Learn all configuration options
- [Transformation Guide](transformation_guide.md) - Write custom transformations
- [Testing Guide](testing_guide.md) - Test your pipelines
- [Examples](../examples/user_analytics/README.md) - Explore the user analytics example

## Common Tasks

### Validate Configuration

```bash
python scripts/validate_config.py configs/examples/user_analytics.yaml
```

### Run Tests

```bash
# All tests
make test

# Unit tests only
make test-unit

# With coverage
make test-coverage
```

### Format Code

```bash
make format
```

### Lint Code

```bash
make lint
```

## Troubleshooting

### Java Not Found

Install Java 8 or 11 and set `JAVA_HOME`:

```bash
export JAVA_HOME=/path/to/java
```

### Memory Issues

Increase Spark driver memory in your config:

```yaml
spark:
  config:
    spark.driver.memory: "4g"
```

### Module Not Found

Make sure you've installed the package:

```bash
pip install -e .
```

## Getting Help

- Check the [configuration guide](configuration_guide.md) for detailed options
- Look at [examples](../examples/user_analytics/README.md) for working code
- Run `make help` to see all available commands
