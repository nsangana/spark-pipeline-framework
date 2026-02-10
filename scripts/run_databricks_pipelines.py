#!/usr/bin/env python3
"""
Run Databricks pipelines to create tables in nsangana_catalog.spark_pipeline_test

This script:
1. Verifies data exists in the volume
2. Runs all Databricks pipeline configurations
3. Creates tables in the schema
4. Displays results and table information
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from databricks.connect import DatabricksSession
from spark_pipeline.core.engine import PipelineEngine


def verify_volume_data(spark, volume_path: str):
    """Verify that data exists in the volume.

    Args:
        spark: Databricks SparkSession
        volume_path: Path to volume

    Returns:
        True if all datasets exist
    """
    print("\n🔍 Verifying data in volume...")

    datasets = ["user_events", "user_profiles", "products", "transactions"]
    all_exist = True

    for dataset in datasets:
        path = f"{volume_path}/{dataset}"
        try:
            df = spark.read.format("delta").load(path)
            count = df.count()
            print(f"  ✓ {dataset}: {count} rows")
        except Exception as e:
            print(f"  ✗ {dataset}: NOT FOUND - {e}")
            all_exist = False

    return all_exist


def run_pipeline(config_path: str, pipeline_name: str):
    """Run a single pipeline.

    Args:
        config_path: Path to pipeline config
        pipeline_name: Name for display

    Returns:
        PipelineResult
    """
    print(f"\n{'=' * 70}")
    print(f"Running: {pipeline_name}")
    print(f"{'=' * 70}")

    try:
        engine = PipelineEngine(config_path)
        result = engine.execute()

        if result.status == "success":
            print(f"\n✅ SUCCESS: {pipeline_name}")
            print(f"  Rows written: {result.metrics.get('rows_written', 'N/A')}")
            print(f"  Validation passed: {result.validation_passed}")
        else:
            print(f"\n❌ FAILED: {pipeline_name}")
            print(f"  Error: {result.error}")

        return result

    except Exception as e:
        print(f"\n❌ ERROR running {pipeline_name}: {e}")
        import traceback
        traceback.print_exc()
        return None


def display_table_info(spark, catalog: str, schema: str):
    """Display information about created tables.

    Args:
        spark: Databricks SparkSession
        catalog: Catalog name
        schema: Schema name
    """
    print(f"\n{'=' * 70}")
    print(f"Tables in {catalog}.{schema}")
    print(f"{'=' * 70}")

    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

        if not tables:
            print("  No tables found")
            return

        for row in tables:
            table_name = row.tableName
            full_name = f"{catalog}.{schema}.{table_name}"

            # Get row count
            try:
                count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()[0]["cnt"]
                print(f"\n  📊 {table_name}")
                print(f"     Rows: {count}")

                # Get columns
                columns = spark.sql(f"DESCRIBE {full_name}").collect()
                print(f"     Columns: {len(columns)}")
                print(f"     Schema: {', '.join([col['col_name'] for col in columns[:5]])}...")

            except Exception as e:
                print(f"  ✗ {table_name}: Error getting info - {e}")

    except Exception as e:
        print(f"  ✗ Error listing tables: {e}")


def main():
    """Main function."""
    print("=" * 70)
    print("Running Databricks Pipelines")
    print("=" * 70)

    # Configuration
    volume_path = "/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test"
    catalog = "nsangana_catalog"
    schema = "spark_pipeline_test"
    profile = "spark-pipeline-testing"

    print(f"\n📁 Data source: {volume_path}")
    print(f"📊 Target: {catalog}.{schema}")
    print(f"🔐 Profile: {profile}")

    # Pipeline configurations
    pipelines = [
        {
            "name": "User Analytics",
            "config": "configs/databricks/user_analytics_databricks.yaml",
            "table": "user_daily_metrics"
        },
        {
            "name": "Product Analytics",
            "config": "configs/databricks/product_analytics_databricks.yaml",
            "table": "product_performance_metrics"
        },
        {
            "name": "Segment Analytics",
            "config": "configs/databricks/segment_analytics_databricks.yaml",
            "table": "segment_performance_metrics"
        }
    ]

    # Connect to Databricks
    print(f"\n🔌 Connecting to Databricks...")
    spark = DatabricksSession.builder.profile(profile).getOrCreate()
    print(f"✓ Connected (Spark {spark.version})")

    try:
        # Verify source data
        if not verify_volume_data(spark, volume_path):
            print("\n❌ ERROR: Source data not found in volume!")
            print(f"\n💡 Run this first:")
            print(f"   python scripts/create_sample_data_in_volume.py")
            sys.exit(1)

        print(f"\n✓ All source data verified")

        # Run all pipelines
        results = []
        for pipeline in pipelines:
            result = run_pipeline(pipeline["config"], pipeline["name"])
            results.append({
                "name": pipeline["name"],
                "table": pipeline["table"],
                "result": result
            })

        # Display summary
        print(f"\n{'=' * 70}")
        print("SUMMARY")
        print(f"{'=' * 70}")

        success_count = sum(1 for r in results if r["result"] and r["result"].status == "success")
        failed_count = len(results) - success_count

        print(f"\n✅ Successful: {success_count}/{len(results)}")
        print(f"❌ Failed: {failed_count}/{len(results)}")

        if success_count > 0:
            print(f"\n📊 Tables created:")
            for r in results:
                if r["result"] and r["result"].status == "success":
                    rows = r["result"].metrics.get("rows_written", "?")
                    print(f"  ✓ {catalog}.{schema}.{r['table']} ({rows} rows)")

        # Display detailed table information
        if success_count > 0:
            display_table_info(spark, catalog, schema)

        print(f"\n{'=' * 70}")
        if failed_count == 0:
            print("✅ ALL PIPELINES COMPLETED SUCCESSFULLY!")
        else:
            print("⚠️  SOME PIPELINES FAILED")
        print(f"{'=' * 70}")

        print(f"\n💡 Query your tables:")
        print(f"   spark.sql('SELECT * FROM {catalog}.{schema}.user_daily_metrics').show()")
        print(f"\n💡 Or use Databricks SQL:")
        print(f"   SELECT * FROM {catalog}.{schema}.user_daily_metrics LIMIT 10;")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
