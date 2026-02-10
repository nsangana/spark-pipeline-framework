#!/usr/bin/env python3
"""Generate sample data for user analytics example."""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def generate_events_data(spark: SparkSession, output_path: str, num_events: int = 1000):
    """Generate sample events data.

    Args:
        spark: SparkSession
        output_path: Path to write events data
        num_events: Number of events to generate
    """
    print(f"Generating {num_events} events...")

    event_types = ["page_view", "click", "purchase", "search", "add_to_cart"]
    user_ids = list(range(1, 51))  # 50 users

    # Generate events
    events = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_events):
        user_id = choice(user_ids)
        event_type = choice(event_types)
        timestamp = base_date + timedelta(
            days=randint(0, 6), hours=randint(0, 23), minutes=randint(0, 59)
        )

        events.append((i + 1, user_id, event_type, timestamp, f"properties_{i}"))

    # Create DataFrame
    schema = StructType(
        [
            StructField("event_id", IntegerType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("event_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("properties", StringType(), True),
        ]
    )

    events_df = spark.createDataFrame(events, schema)

    # Write to Parquet
    events_df.write.mode("overwrite").parquet(output_path)
    print(f"✓ Wrote {events_df.count()} events to {output_path}")


def generate_user_profiles_data(spark: SparkSession, output_path: str):
    """Generate sample user profiles data.

    Args:
        spark: SparkSession
        output_path: Path to write user profiles data
    """
    print("Generating user profiles...")

    segments = ["free", "premium", "enterprise"]
    names = [
        "Alice",
        "Bob",
        "Charlie",
        "David",
        "Eve",
        "Frank",
        "Grace",
        "Henry",
        "Ivy",
        "Jack",
    ]

    # Generate profiles for 50 users
    profiles = []
    base_signup_date = datetime(2023, 1, 1)

    for i in range(1, 51):
        user_id = i
        name = names[i % len(names)] + f"_{i}"
        segment = choice(segments)
        signup_date = (base_signup_date + timedelta(days=randint(0, 365))).date()

        profiles.append((user_id, name, segment, signup_date))

    # Create DataFrame
    schema = StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("segment", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )

    profiles_df = spark.createDataFrame(profiles, schema)

    # Write to Parquet
    profiles_df.write.mode("overwrite").parquet(output_path)
    print(f"✓ Wrote {profiles_df.count()} user profiles to {output_path}")


def main():
    """Main function."""
    # Get output directory from args or use default
    if len(sys.argv) > 1:
        output_dir = Path(sys.argv[1])
    else:
        output_dir = Path(__file__).parent.parent / "data"

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}")

    # Create Spark session
    spark = (
        SparkSession.builder.appName("GenerateSampleData")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # Generate data
        generate_events_data(spark, str(output_dir / "events"), num_events=1000)
        generate_user_profiles_data(spark, str(output_dir / "user_profiles"))

        print("\n✓ Sample data generation completed successfully!")
        print(f"\nTo run the pipeline, set environment variables:")
        print(f"  export DATA_PATH={output_dir}")
        print(f"  export OUTPUT_PATH={output_dir.parent}/output")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
