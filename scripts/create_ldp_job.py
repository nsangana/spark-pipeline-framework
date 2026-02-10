#!/usr/bin/env python3
"""
Create a Databricks Job to orchestrate a Lakeflow Declarative Pipeline.

This script creates a job that runs an existing LDP pipeline.
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from spark_pipeline.ldp_generator import LDPDeployer, load_config


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Create orchestration job for LDP pipeline"
    )
    parser.add_argument(
        "pipeline_id",
        help="Pipeline ID to orchestrate",
    )
    parser.add_argument(
        "--job-name",
        required=True,
        help="Name for the orchestration job",
    )
    parser.add_argument(
        "--schedule",
        help="Cron schedule (e.g., '0 2 * * *' for daily at 2 AM UTC)",
    )
    parser.add_argument(
        "--config-file",
        help="Path to LDP configuration file (ldp_config.yaml)",
    )
    parser.add_argument(
        "--profile",
        help="Databricks profile (default: from config or DATABRICKS_PROFILE)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Trigger a run immediately after creating the job",
    )

    args = parser.parse_args()

    # Load configuration
    ldp_config = load_config(
        config_file=args.config_file,
        profile=args.profile,
    )

    print("=" * 70)
    print("Create Orchestration Job for LDP Pipeline")
    print("=" * 70)
    print(f"\n📊 Pipeline ID: {args.pipeline_id}")
    print(f"🔧 Job Name: {args.job_name}")
    if args.schedule:
        print(f"⏰ Schedule: {args.schedule}")
    print(f"🔐 Profile: {ldp_config.profile}")
    print(f"👤 Workspace User: {ldp_config.get_workspace_user()}")

    try:
        deployer = LDPDeployer(config=ldp_config)

        # Create the job
        print(f"\n{'=' * 70}")
        print("Creating Job...")
        print(f"{'=' * 70}")

        result = deployer.create_job_for_pipeline(
            job_name=args.job_name,
            pipeline_id=args.pipeline_id,
            schedule_cron=args.schedule,
            description=f"Orchestration job for pipeline {args.pipeline_id}",
        )

        job_id = result.get("job_id")
        print(f"\n✅ Job created successfully")
        print(f"   Job ID: {job_id}")
        print(f"   Job Name: {args.job_name}")

        if args.schedule:
            print(f"   Schedule: {args.schedule}")
            print(f"   Status: ACTIVE")
        else:
            print(f"   Trigger: Manual")

        # Run if requested
        if args.run:
            print(f"\n{'=' * 70}")
            print("Triggering Pipeline Run...")
            print(f"{'=' * 70}")

            run_result = deployer.run_pipeline(pipeline_id=args.pipeline_id)
            update_id = run_result.get("update_id")

            print(f"\n✅ Pipeline run started")
            print(f"   Update ID: {update_id}")

        print(f"\n{'=' * 70}")
        print("✅ COMPLETE")
        print(f"{'=' * 70}")

        print(f"\n💡 View in Databricks UI:")
        print(f"   Workflows > Jobs > {args.job_name}")
        print(f"   Workflows > Delta Live Tables > [Your Pipeline]")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
