#!/usr/bin/env python3
"""
Generate Lakeflow Declarative Pipelines from YAML configs and deploy to Databricks.

This script:
1. Reads Spark Pipeline Framework YAML configurations
2. Generates LDP SQL code
3. Deploys pipelines to Databricks workspace
4. Creates orchestration jobs (optional)
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from spark_pipeline.ldp_generator import LDPSQLGenerator, LDPDeployer, load_config


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate and deploy Lakeflow Declarative Pipelines"
    )
    parser.add_argument(
        "config",
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--output-dir",
        default="ldp_generated",
        help="Directory to save generated SQL files (default: ldp_generated)",
    )
    parser.add_argument(
        "--config-file",
        help="Path to LDP configuration file (ldp_config.yaml)",
    )
    parser.add_argument(
        "--catalog",
        help="Unity Catalog name (default: from config or DATABRICKS_CATALOG)",
    )
    parser.add_argument(
        "--schema",
        help="Schema name (default: from config or DATABRICKS_SCHEMA)",
    )
    parser.add_argument(
        "--profile",
        help="Databricks profile (default: from config or DATABRICKS_PROFILE)",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy pipeline to Databricks workspace",
    )
    parser.add_argument(
        "--create-job",
        action="store_true",
        help="Create orchestration job for the pipeline",
    )
    parser.add_argument(
        "--schedule",
        help="Cron schedule for job (e.g., '0 0 * * *' for daily at midnight UTC)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the pipeline after deployment",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run pipeline in continuous mode",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Deploy in production mode (default is development)",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ Error: Config file not found: {config_path}")
        sys.exit(1)

    # Load LDP configuration
    ldp_config = load_config(
        config_file=args.config_file,
        profile=args.profile,
        catalog=args.catalog,
        schema=args.schema,
    )

    print("=" * 70)
    print("Lakeflow Declarative Pipeline Generator & Deployer")
    print("=" * 70)
    print(f"\n📄 Pipeline Config: {config_path}")
    print(f"📁 Output Directory: {args.output_dir}")
    print(f"📊 Target: {ldp_config.catalog}.{ldp_config.schema}")
    print(f"🔐 Profile: {ldp_config.profile}")
    print(f"👤 Workspace User: {ldp_config.get_workspace_user()}")
    print(f"📂 Workspace Path: {ldp_config.get_workspace_base_path()}")

    # Step 1: Generate SQL
    print(f"\n{'=' * 70}")
    print("Step 1: Generate LDP SQL")
    print(f"{'=' * 70}")

    try:
        generator = LDPSQLGenerator(str(config_path))
        pipeline_name = generator.pipeline_name

        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate and save SQL
        sql_file = output_dir / f"{pipeline_name}.sql"
        generator.save_sql(str(sql_file))

        print(f"\n✅ SQL generated successfully")
        print(f"   File: {sql_file}")

        # Display preview of generated SQL
        with open(sql_file, "r") as f:
            sql_content = f.read()
            lines = sql_content.split("\n")
            preview_lines = min(20, len(lines))
            print(f"\n📝 Preview (first {preview_lines} lines):")
            print("-" * 70)
            print("\n".join(lines[:preview_lines]))
            if len(lines) > preview_lines:
                print(f"... ({len(lines) - preview_lines} more lines)")
            print("-" * 70)

    except Exception as e:
        print(f"\n❌ Error generating SQL: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Step 2: Deploy (if requested)
    if args.deploy:
        print(f"\n{'=' * 70}")
        print("Step 2: Deploy Pipeline to Databricks")
        print(f"{'=' * 70}")

        try:
            deployer = LDPDeployer(config=ldp_config)

            result = deployer.create_pipeline(
                name=pipeline_name,
                sql_file_path=str(sql_file),
                catalog=ldp_config.catalog,
                schema=ldp_config.schema,
                description=f"Generated from {config_path.name}",
                continuous=args.continuous,
                development=not args.production,
            )

            pipeline_id = result.get("pipeline_id")
            print(f"\n✅ Pipeline deployed successfully")
            print(f"   Pipeline ID: {pipeline_id}")
            print(f"   Mode: {'Production' if args.production else 'Development'}")
            print(f"   Continuous: {args.continuous}")

            # Step 3: Create Job (if requested)
            if args.create_job:
                print(f"\n{'=' * 70}")
                print("Step 3: Create Orchestration Job")
                print(f"{'=' * 70}")

                job_name = f"{pipeline_name}_job"
                job_result = deployer.create_job_for_pipeline(
                    job_name=job_name,
                    pipeline_id=pipeline_id,
                    schedule_cron=args.schedule,
                    description=f"Orchestration job for {pipeline_name}",
                )

                job_id = job_result.get("job_id")
                print(f"\n✅ Job created successfully")
                print(f"   Job ID: {job_id}")
                print(f"   Job Name: {job_name}")
                if args.schedule:
                    print(f"   Schedule: {args.schedule}")

            # Step 4: Run (if requested)
            if args.run:
                print(f"\n{'=' * 70}")
                print("Step 4: Run Pipeline")
                print(f"{'=' * 70}")

                run_result = deployer.run_pipeline(pipeline_id=pipeline_id)
                update_id = run_result.get("update_id")
                print(f"\n✅ Pipeline run started")
                print(f"   Update ID: {update_id}")
                print(f"\n💡 Monitor progress in Databricks UI:")
                print(f"   Workflows > Delta Live Tables > {pipeline_name}")

        except Exception as e:
            print(f"\n❌ Error deploying pipeline: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    print(f"\n{'=' * 70}")
    print("✅ COMPLETE")
    print(f"{'=' * 70}")

    if not args.deploy:
        print(f"\n💡 To deploy this pipeline, run:")
        print(f"   python {__file__} {args.config} --deploy")
        print(f"\n💡 To deploy and create a scheduled job:")
        print(
            f"   python {__file__} {args.config} --deploy --create-job --schedule '0 0 * * *'"
        )
        print(f"\n💡 To deploy and run immediately:")
        print(f"   python {__file__} {args.config} --deploy --run")


if __name__ == "__main__":
    main()
