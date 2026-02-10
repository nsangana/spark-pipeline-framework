# Quick Query Reference

Tables created in `nsangana_catalog.spark_pipeline_test`

## 📊 Tables Overview

```sql
-- List all tables
SHOW TABLES IN nsangana_catalog.spark_pipeline_test;

-- Table details
DESCRIBE EXTENDED nsangana_catalog.spark_pipeline_test.user_daily_metrics;
DESCRIBE EXTENDED nsangana_catalog.spark_pipeline_test.product_performance_metrics;
DESCRIBE EXTENDED nsangana_catalog.spark_pipeline_test.segment_performance_metrics;
```

## 1️⃣ user_daily_metrics (846 rows)

### Schema
- user_id, date, total_events, unique_event_types
- page_views, clicks, purchases, total_revenue
- session_duration_minutes
- name, email, segment, country, signup_date
- days_since_signup, engagement_score

### Quick Queries

```sql
-- Top users by engagement
SELECT user_id, name, segment, SUM(engagement_score) as total_engagement
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY user_id, name, segment
ORDER BY total_engagement DESC
LIMIT 10;

-- Revenue by segment
SELECT segment, SUM(total_revenue) as revenue, COUNT(DISTINCT user_id) as users
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY segment
ORDER BY revenue DESC;

-- Daily active users
SELECT date, COUNT(DISTINCT user_id) as active_users, SUM(total_revenue) as revenue
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY date
ORDER BY date DESC;

-- Users with purchases
SELECT user_id, name, segment, SUM(purchases) as total_purchases, SUM(total_revenue) as revenue
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
WHERE purchases > 0
GROUP BY user_id, name, segment
ORDER BY revenue DESC;
```

## 2️⃣ product_performance_metrics (50 rows)

### Schema
- product_id, product_name, category, price
- unique_viewers, clicks, add_to_carts, purchases_from_events
- total_quantity_sold, total_revenue, transaction_count
- click_to_cart_rate, cart_to_purchase_rate, avg_revenue_per_unit

### Quick Queries

```sql
-- Top products by revenue
SELECT product_name, category, total_revenue, transaction_count
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
ORDER BY total_revenue DESC
LIMIT 10;

-- Best converting products
SELECT product_name, category,
       click_to_cart_rate,
       cart_to_purchase_rate,
       total_revenue
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
WHERE clicks > 0
ORDER BY cart_to_purchase_rate DESC
LIMIT 10;

-- Category performance
SELECT category,
       COUNT(*) as num_products,
       SUM(total_revenue) as category_revenue,
       AVG(click_to_cart_rate) as avg_conversion
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
GROUP BY category
ORDER BY category_revenue DESC;

-- Low performing products
SELECT product_name, category, unique_viewers, clicks, total_revenue
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
WHERE unique_viewers > 10 AND total_revenue < 100
ORDER BY unique_viewers DESC;
```

## 3️⃣ segment_performance_metrics (3 rows)

### Schema
- segment (free/premium/enterprise)
- total_users, total_events, total_active_days
- avg_events_per_user, avg_active_days_per_user
- total_purchases, avg_purchases_per_user
- total_revenue, avg_revenue_per_user
- avg_transaction_value, engagement_score

### Quick Queries

```sql
-- All segments overview
SELECT * FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics
ORDER BY engagement_score DESC;

-- Segment comparison
SELECT
    segment,
    total_users,
    ROUND(avg_revenue_per_user, 2) as avg_revenue,
    ROUND(avg_purchases_per_user, 2) as avg_purchases,
    ROUND(engagement_score, 2) as engagement
FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics
ORDER BY avg_revenue DESC;

-- Revenue per user by segment
SELECT
    segment,
    CONCAT('$', ROUND(avg_revenue_per_user, 2)) as avg_revenue_per_user,
    CONCAT('$', ROUND(total_revenue, 2)) as total_revenue,
    total_users
FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics
ORDER BY avg_revenue_per_user DESC;
```

## 🔗 Join Queries

### User + Product Analysis
```sql
SELECT
    u.segment,
    p.category,
    COUNT(DISTINCT u.user_id) as users,
    SUM(u.total_revenue) as revenue
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics u
CROSS JOIN nsangana_catalog.spark_pipeline_test.product_performance_metrics p
GROUP BY u.segment, p.category
ORDER BY revenue DESC
LIMIT 20;
```

### Segment + User Detail
```sql
SELECT
    s.segment,
    s.total_users,
    s.avg_revenue_per_user as segment_avg_revenue,
    u.user_id,
    u.name,
    SUM(u.total_revenue) as user_revenue
FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics s
JOIN nsangana_catalog.spark_pipeline_test.user_daily_metrics u ON s.segment = u.segment
GROUP BY s.segment, s.total_users, s.avg_revenue_per_user, u.user_id, u.name
ORDER BY user_revenue DESC
LIMIT 20;
```

## 📈 Business Intelligence Queries

### Customer Lifetime Value by Segment
```sql
SELECT
    segment,
    COUNT(DISTINCT user_id) as customers,
    SUM(total_revenue) as total_revenue,
    AVG(total_revenue) as avg_customer_value,
    PERCENTILE(total_revenue, 0.5) as median_customer_value
FROM (
    SELECT user_id, segment, SUM(total_revenue) as total_revenue
    FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
    GROUP BY user_id, segment
)
GROUP BY segment
ORDER BY avg_customer_value DESC;
```

### Product Mix Analysis
```sql
SELECT
    category,
    COUNT(*) as products,
    SUM(total_revenue) as revenue,
    ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (), 2) as revenue_pct,
    AVG(cart_to_purchase_rate) as avg_conversion
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
GROUP BY category
ORDER BY revenue DESC;
```

### User Engagement Trend
```sql
SELECT
    date,
    segment,
    COUNT(DISTINCT user_id) as daily_active_users,
    AVG(total_events) as avg_events_per_user,
    AVG(engagement_score) as avg_engagement
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY date, segment
ORDER BY date DESC, segment;
```

## 🔧 Data Exploration

### Row counts
```sql
SELECT COUNT(*) as count FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics;
SELECT COUNT(*) as count FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics;
SELECT COUNT(*) as count FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics;
```

### Sample data
```sql
SELECT * FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics LIMIT 5;
SELECT * FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics LIMIT 5;
SELECT * FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics;
```

### Column lists
```sql
SELECT column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = 'nsangana_catalog'
  AND table_schema = 'spark_pipeline_test'
  AND table_name = 'user_daily_metrics';
```

## 🐍 PySpark Queries

```python
from databricks.connect import DatabricksSession

# Connect
spark = DatabricksSession.builder.profile("spark-pipeline-testing").getOrCreate()

# Read tables
users_df = spark.sql("SELECT * FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics")
products_df = spark.sql("SELECT * FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics")
segments_df = spark.sql("SELECT * FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics")

# Display
users_df.show(10)
products_df.show(10)
segments_df.show()

# Analysis
from pyspark.sql import functions as F

# Top users by revenue
users_df.groupBy("user_id", "name", "segment") \
    .agg(F.sum("total_revenue").alias("revenue")) \
    .orderBy(F.desc("revenue")) \
    .show(10)

# Products by category
products_df.groupBy("category") \
    .agg(
        F.count("*").alias("products"),
        F.sum("total_revenue").alias("revenue"),
        F.avg("click_to_cart_rate").alias("avg_conversion")
    ) \
    .orderBy(F.desc("revenue")) \
    .show()
```

## 💾 Export Data

```python
# Export to CSV
users_df.write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test/exports/users.csv")

# Export to Parquet
products_df.write.format("parquet") \
    .mode("overwrite") \
    .save("/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test/exports/products.parquet")
```

## 🧹 Cleanup (if needed)

```sql
-- Drop tables
DROP TABLE IF EXISTS nsangana_catalog.spark_pipeline_test.user_daily_metrics;
DROP TABLE IF EXISTS nsangana_catalog.spark_pipeline_test.product_performance_metrics;
DROP TABLE IF EXISTS nsangana_catalog.spark_pipeline_test.segment_performance_metrics;

-- Re-run pipelines
-- python scripts/run_databricks_pipelines.py
```

---

**Tip**: Save these queries in a Databricks notebook for easy access!
