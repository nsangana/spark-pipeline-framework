# Spark Declarative Pipeline Framework

A production-ready Spark pipeline framework that allows you to define data pipelines declaratively using YAML configuration. The framework integrates data quality testing with pytest, ensuring data is only written to Delta Lake tables if all validations pass.

## ✅ Verified on Azure Databricks

**All tests passed!** The framework is fully compatible with Azure Databricks using:
- **Serverless compute** for fast, scalable execution
- **Unity Catalog** for managed tables and governance
- **Databricks Connect** for remote execution

[See Databricks Quick Start →](DATABRICKS_QUICKSTART.md)

## Features

- **Declarative Configuration**: Define pipelines in YAML with no boilerplate code
- **Delta Lake Storage**: ACID transactions with incremental updates and overwrites
- **Data Quality Gates**: Integrated validation framework that gates writes
- **Flexible Transformations**: SQL, Python functions, and built-in operations
- **pytest Integration**: Custom data quality tests as validation rules
- **Conditional Writes**: Data is only written if ALL validations pass
- **Parquet Support**: Read from various Parquet data sources

## Quick Start

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd claude_first_project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e .
```

### Run Example Pipeline

```bash
# Set up environment variables
export DATA_PATH=$(pwd)/examples/user_analytics/data
export OUTPUT_PATH=$(pwd)/output

# Run the user analytics example
python scripts/run_pipeline.py configs/examples/user_analytics.yaml
```

## Project Structure

```
claude_first_project/
├── spark_pipeline/              # Main package
│   ├── core/                    # Engine, config parser, readers, writers
│   ├── transformations/         # Transformation registry and implementations
│   ├── validation/              # Data quality validation framework
│   └── utils/                   # Spark session management, utilities
├── configs/                     # Pipeline YAML configurations
│   ├── schemas/                 # JSON schemas for validation
│   └── examples/                # Example pipeline configs
├── transformations/             # User-defined transformation functions
├── tests/                       # Unit, integration, and data quality tests
├── examples/                    # Complete example projects with data
└── scripts/                     # CLI scripts for running pipelines
```

## Example Pipeline Configuration

```yaml
pipeline:
  name: "user_analytics"

spark:
  app_name: "User Analytics Pipeline"
  master: "local[*]"
  config:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"

sources:
  - name: "events"
    type: "parquet"
    path: "${DATA_PATH}/events"

transformations:
  - name: "daily_metrics"
    type: "python"
    inputs: ["events"]
    output: "metrics"
    function: "transformations.user_analytics.calculate_daily_metrics"

validation:
  enabled: true
  fail_on_error: true
  rules:
    - name: "check_nulls"
      type: "null_check"
      column: "user_id"
      threshold: 0.0

target:
  type: "delta"
  path: "${OUTPUT_PATH}/user_metrics"
  mode: "overwrite"
  partition_by: ["date"]
```

## Core Concepts

### Conditional Writes

The framework ensures data quality by only writing data if ALL validation rules pass:

1. Data is read from sources
2. Transformations are applied
3. Validation rules are executed
4. **If all validations pass**: Data is written to Delta Lake
5. **If any validation fails**: Error is raised, NO data is written

This prevents data corruption and ensures data quality in production pipelines.

### Validation Rules

Built-in validation rules include:

- **null_check**: Check null percentage against threshold
- **range_check**: Validate numeric ranges
- **row_count**: Min/max row count validation

You can also write custom pytest tests that run as validation rules.

### Transformations

Three types of transformations are supported:

- **SQL**: Standard SQL transformations
- **Python**: Custom Python functions
- **Built-in**: Join, filter, aggregate operations

## Testing

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Run with coverage
pytest --cov=spark_pipeline --cov-report=html
```

## Documentation

- [Quick Start Guide](docs/quickstart.md)
- [Configuration Reference](docs/configuration_guide.md)
- [Writing Transformations](docs/transformation_guide.md)
- [Testing Guide](docs/testing_guide.md)

## Development

```bash
# Format code
make format

# Lint code
make lint

# Type check
make typecheck

# Run all quality checks
make check
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read CONTRIBUTING.md for guidelines.
