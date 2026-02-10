"""Transformation functions for user analytics pipeline."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_daily_metrics(events_df: DataFrame) -> DataFrame:
    """Calculate daily user engagement metrics.

    Args:
        events_df: DataFrame with columns: user_id, event_type, timestamp, properties

    Returns:
        DataFrame with daily aggregated metrics per user
    """
    # Extract date from timestamp
    df = events_df.withColumn("date", F.to_date(F.col("timestamp")))

    # Calculate metrics by user and date
    daily_metrics = (
        df.groupBy("user_id", "date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("event_type").alias("unique_event_types"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias(
                "page_views"
            ),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias(
                "clicks"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
            F.min("timestamp").alias("first_event_time"),
            F.max("timestamp").alias("last_event_time"),
        )
    )

    # Calculate session duration in minutes
    daily_metrics = daily_metrics.withColumn(
        "session_duration_minutes",
        (
            F.unix_timestamp("last_event_time")
            - F.unix_timestamp("first_event_time")
        )
        / 60,
    )

    return daily_metrics


def enrich_with_profiles(metrics_df: DataFrame, profiles_df: DataFrame) -> DataFrame:
    """Enrich daily metrics with user profile information.

    Args:
        metrics_df: DataFrame with daily metrics
        profiles_df: DataFrame with user profiles (user_id, name, segment, signup_date)

    Returns:
        Enriched DataFrame with profile information
    """
    # Join metrics with profiles
    enriched = metrics_df.join(profiles_df, on="user_id", how="left")

    # Calculate days since signup
    enriched = enriched.withColumn(
        "days_since_signup",
        F.datediff(F.col("date"), F.col("signup_date")),
    )

    # Add engagement score (simple calculation)
    enriched = enriched.withColumn(
        "engagement_score",
        (
            F.col("page_views") * 1
            + F.col("clicks") * 2
            + F.col("purchases") * 10
        ),
    )

    return enriched


def filter_active_users(df: DataFrame, min_events: int = 5) -> DataFrame:
    """Filter to only include active users.

    Args:
        df: DataFrame with user metrics
        min_events: Minimum number of events to be considered active

    Returns:
        Filtered DataFrame with only active users
    """
    return df.filter(F.col("total_events") >= min_events)


def aggregate_by_segment(df: DataFrame) -> DataFrame:
    """Aggregate metrics by user segment.

    Args:
        df: DataFrame with user metrics and segments

    Returns:
        DataFrame with segment-level aggregations
    """
    segment_metrics = (
        df.groupBy("segment", "date")
        .agg(
            F.count("user_id").alias("active_users"),
            F.sum("total_events").alias("total_events"),
            F.sum("page_views").alias("total_page_views"),
            F.sum("clicks").alias("total_clicks"),
            F.sum("purchases").alias("total_purchases"),
            F.avg("engagement_score").alias("avg_engagement_score"),
            F.avg("session_duration_minutes").alias("avg_session_duration"),
        )
    )

    return segment_metrics
