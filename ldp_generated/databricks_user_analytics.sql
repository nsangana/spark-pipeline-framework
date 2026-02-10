-- Lakeflow Declarative Pipeline: databricks_user_analytics
-- Generated from: user_analytics_databricks.yaml
-- Description: User engagement analytics pipeline on Databricks
--
-- This pipeline was auto-generated from Spark Pipeline Framework YAML configuration.

-- Source: events (streaming)
CREATE OR REFRESH STREAMING TABLE events
AS SELECT * FROM STREAM delta.`/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test/user_events`;

-- Source: user_profiles (materialized view - dimension table)
CREATE OR REFRESH MATERIALIZED VIEW user_profiles
AS SELECT * FROM delta.`/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test/user_profiles`;

-- Transformation: daily_metrics (streaming transformation)
CREATE OR REFRESH STREAMING TABLE daily_metrics
AS SELECT
  user_id,
  DATE(timestamp) as date,
  COUNT(*) as total_events,
  approx_count_distinct(event_type) as unique_event_types,
  SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
  SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
  SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) as total_revenue,
  MIN(timestamp) as first_event_time,
  MAX(timestamp) as last_event_time
FROM STREAM(events)
GROUP BY user_id, DATE(timestamp);

-- Transformation: metrics_with_duration (streaming transformation)
CREATE OR REFRESH STREAMING TABLE metrics_with_duration
AS SELECT
  *,
  (UNIX_TIMESTAMP(last_event_time) - UNIX_TIMESTAMP(first_event_time)) / 60 as session_duration_minutes
FROM STREAM(daily_metrics);

-- Transformation: enriched_metrics (streaming transformation)
CREATE OR REFRESH STREAMING TABLE enriched_metrics
AS SELECT
  m.*,
  p.name,
  p.email,
  p.segment,
  p.country,
  p.signup_date,
  DATEDIFF(m.date, p.signup_date) as days_since_signup
FROM STREAM(metrics_with_duration) m
LEFT JOIN user_profiles p ON m.user_id = p.user_id;

-- Transformation: final_metrics (final aggregation)
CREATE OR REFRESH MATERIALIZED VIEW final_metrics(
  CONSTRAINT check_user_id_not_null EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT check_positive_events EXPECT (total_events >= 1) ON VIOLATION DROP ROW,
  CONSTRAINT check_engagement_score_range EXPECT (engagement_score >= 0 AND engagement_score <= 100000) ON VIOLATION DROP ROW
)
AS SELECT
  *,
  (page_views * 1 + clicks * 2 + purchases * 10) as engagement_score
FROM enriched_metrics;