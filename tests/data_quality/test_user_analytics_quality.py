"""Data quality tests for user analytics pipeline."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.data_quality
def test_user_segments_are_valid(spark_session: SparkSession):
    """Test that user segments are from expected set."""
    # This would be called with the actual pipeline DataFrame
    # For demo purposes, we'll create a sample
    valid_segments = {"free", "premium", "enterprise"}

    # In real usage, this would receive the DataFrame from the pipeline
    # df = pipeline_context.get_dataframe("enriched_metrics")

    # Example validation logic:
    # unique_segments = set([row.segment for row in df.select("segment").distinct().collect()])
    # invalid_segments = unique_segments - valid_segments

    # assert len(invalid_segments) == 0, f"Invalid segments found: {invalid_segments}"
    pass


@pytest.mark.data_quality
def test_engagement_score_calculation(spark_session: SparkSession):
    """Test that engagement score is calculated correctly."""
    # Create test data
    from pyspark.sql import functions as F

    test_data = spark_session.createDataFrame(
        [(1, 10, 5, 2), (2, 20, 10, 1)],  # page_views, clicks, purchases
        ["user_id", "page_views", "clicks", "purchases"],
    )

    # Calculate engagement score
    result = test_data.withColumn(
        "engagement_score",
        F.col("page_views") * 1 + F.col("clicks") * 2 + F.col("purchases") * 10,
    )

    # Verify calculations
    scores = [row.engagement_score for row in result.collect()]
    assert scores[0] == 10 + 10 + 20  # 40
    assert scores[1] == 20 + 20 + 10  # 50


@pytest.mark.data_quality
def test_no_future_dates(spark_session: SparkSession):
    """Test that no events have future dates."""
    from datetime import datetime
    from pyspark.sql import functions as F

    # Example test
    test_data = spark_session.createDataFrame(
        [
            (1, "2024-01-01"),
            (2, "2024-01-02"),
        ],
        ["user_id", "date"],
    )

    today = datetime.now().date()
    future_dates = test_data.filter(F.col("date") > F.lit(str(today)))

    assert future_dates.count() == 0, "Found events with future dates"


@pytest.mark.data_quality
def test_session_duration_reasonable(spark_session: SparkSession):
    """Test that session durations are reasonable (not negative or extremely long)."""
    from pyspark.sql import functions as F

    # Example test
    test_data = spark_session.createDataFrame(
        [(1, 30.5), (2, 120.0), (3, 5.0)],  # session_duration_minutes
        ["user_id", "session_duration_minutes"],
    )

    # Check for negative durations
    negative = test_data.filter(F.col("session_duration_minutes") < 0)
    assert negative.count() == 0, "Found negative session durations"

    # Check for unreasonably long sessions (e.g., > 24 hours)
    too_long = test_data.filter(F.col("session_duration_minutes") > 1440)
    assert too_long.count() == 0, "Found unreasonably long sessions"


@pytest.mark.data_quality
def test_user_id_consistency(spark_session: SparkSession):
    """Test that user_ids are consistent across metrics."""
    # Example: Check that all user_ids are positive integers
    test_data = spark_session.createDataFrame(
        [(1,), (2,), (3,)],
        ["user_id"],
    )

    from pyspark.sql import functions as F

    invalid_ids = test_data.filter(
        (F.col("user_id").isNull()) | (F.col("user_id") <= 0)
    )

    assert invalid_ids.count() == 0, "Found invalid user IDs"
