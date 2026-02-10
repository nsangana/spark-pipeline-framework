# Databricks Pipeline Execution Results

## ✅ Success Summary

**All 3 pipelines executed successfully** and created tables in `nsangana_catalog.spark_pipeline_test`

## 📊 Sample Data Created in Volume

**Location**: `/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test`

| Dataset | Rows | Description |
|---------|------|-------------|
| `user_events` | 1,000 | User activity events (page views, clicks, purchases) |
| `user_profiles` | 100 | User profile information (name, segment, country, age) |
| `products` | 50 | Product catalog (name, category, price, stock, rating) |
| `transactions` | 500 | Purchase transactions (user, product, quantity, amount) |

## 🎯 Tables Created in Schema

**Schema**: `nsangana_catalog.spark_pipeline_test`

### 1. user_daily_metrics
- **Rows**: 846
- **Pipeline**: User Analytics
- **Description**: Daily user engagement metrics with profiles
- **Key Columns**: user_id, date, total_events, page_views, clicks, purchases, total_revenue, segment, engagement_score

**Query Example**:
```sql
SELECT * FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics LIMIT 10;
```

### 2. product_performance_metrics
- **Rows**: 50
- **Pipeline**: Product Analytics
- **Description**: Product performance metrics with conversion rates
- **Key Columns**: product_id, product_name, category, price, unique_viewers, clicks, purchases, total_revenue, conversion rates

**Query Example**:
```sql
SELECT product_name, category, total_revenue, click_to_cart_rate, cart_to_purchase_rate
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
ORDER BY total_revenue DESC
LIMIT 10;
```

### 3. segment_performance_metrics
- **Rows**: 3
- **Pipeline**: Segment Analytics
- **Description**: User segment performance analysis (free, premium, enterprise)
- **Key Columns**: segment, total_users, avg_events_per_user, avg_revenue_per_user, engagement_score

**Query Example**:
```sql
SELECT * FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics
ORDER BY avg_revenue_per_user DESC;
```

## 🚀 Pipeline Execution Details

### Pipeline 1: User Analytics

**Config**: `configs/databricks/user_analytics_databricks.yaml`

**Workflow**:
1. Read `user_events` and `user_profiles` from volume
2. Calculate daily metrics per user (events, clicks, purchases, revenue)
3. Calculate session duration
4. Enrich with user profile data (segment, country, days since signup)
5. Calculate engagement score (page_views × 1 + clicks × 2 + purchases × 10)
6. Validate data quality (nulls, ranges, row count)
7. Write to `user_daily_metrics` table (partitioned by date)
8. Optimize with Z-ordering on user_id

**Validations**:
- ✅ No null user_ids
- ✅ All events counts positive
- ✅ Engagement scores within range [0, 100000]
- ✅ Minimum record count met

**Result**: ✅ **846 rows** written successfully

---

### Pipeline 2: Product Analytics

**Config**: `configs/databricks/product_analytics_databricks.yaml`

**Workflow**:
1. Read `events`, `products`, and `transactions` from volume
2. Calculate product-level metrics (viewers, clicks, add-to-carts, purchases)
3. Enrich with transaction data (quantity sold, revenue)
4. Calculate conversion rates:
   - Click-to-cart rate
   - Cart-to-purchase rate
   - Average revenue per unit
5. Validate data quality
6. Write to `product_performance_metrics` table
7. Optimize with Z-ordering on product_id

**Validations**:
- ✅ No null product_ids
- ✅ All prices positive
- ✅ Minimum record count met

**Result**: ✅ **50 rows** written successfully

---

### Pipeline 3: Segment Analytics

**Config**: `configs/databricks/segment_analytics_databricks.yaml`

**Workflow**:
1. Read `user_profiles`, `transactions`, and `events` from volume
2. Aggregate user activity by user (events, purchases, spend)
3. Aggregate transactions by user (count, revenue, avg value)
4. Combine user profile + activity + transaction data
5. Aggregate by segment (free, premium, enterprise)
6. Calculate engagement score per segment
7. Validate data quality
8. Write to `segment_performance_metrics` table

**Validations**:
- ✅ No null segments
- ✅ All user counts positive
- ✅ Minimum record count met

**Result**: ✅ **3 rows** written successfully (one per segment)

---

## 📈 Sample Queries

### 1. Top 10 Users by Engagement

```sql
SELECT
    user_id,
    name,
    segment,
    SUM(total_events) as total_events,
    SUM(engagement_score) as total_engagement,
    SUM(total_revenue) as total_revenue
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY user_id, name, segment
ORDER BY total_engagement DESC
LIMIT 10;
```

### 2. Product Performance by Category

```sql
SELECT
    category,
    COUNT(*) as num_products,
    SUM(total_revenue) as total_revenue,
    AVG(click_to_cart_rate) as avg_click_to_cart_rate,
    AVG(cart_to_purchase_rate) as avg_cart_to_purchase_rate
FROM nsangana_catalog.spark_pipeline_test.product_performance_metrics
GROUP BY category
ORDER BY total_revenue DESC;
```

### 3. Segment Comparison

```sql
SELECT
    segment,
    total_users,
    ROUND(avg_events_per_user, 2) as avg_events,
    ROUND(avg_revenue_per_user, 2) as avg_revenue,
    ROUND(engagement_score, 2) as engagement
FROM nsangana_catalog.spark_pipeline_test.segment_performance_metrics
ORDER BY engagement_score DESC;
```

### 4. Daily Revenue Trend

```sql
SELECT
    date,
    SUM(total_revenue) as daily_revenue,
    COUNT(DISTINCT user_id) as active_users,
    SUM(purchases) as total_purchases
FROM nsangana_catalog.spark_pipeline_test.user_daily_metrics
GROUP BY date
ORDER BY date DESC;
```

## 🔍 Data Quality Validation

All pipelines enforced strict data quality rules:

| Rule Type | Column | Threshold | Status |
|-----------|--------|-----------|--------|
| Null Check | user_id | 0% | ✅ PASSED |
| Null Check | product_id | 0% | ✅ PASSED |
| Null Check | segment | 0% | ✅ PASSED |
| Range Check | total_events | min: 1 | ✅ PASSED |
| Range Check | price | min: 0 | ✅ PASSED |
| Range Check | engagement_score | 0-100000 | ✅ PASSED |
| Row Count | All tables | min: 1 | ✅ PASSED |

**Key Feature**: If ANY validation failed, the pipeline would **NOT write data** to the table, preventing data corruption.

## 🛠️ How to Run

### Re-create Sample Data

```bash
source venv/bin/activate
export DATABRICKS_CONFIG_PROFILE=spark-pipeline-testing
python scripts/create_sample_data_in_volume.py
```

### Run Pipelines

```bash
python scripts/run_databricks_pipelines.py
```

### Run Individual Pipeline

```bash
python scripts/run_pipeline.py configs/databricks/user_analytics_databricks.yaml
```

## 📁 Files Created

### Scripts
- `scripts/create_sample_data_in_volume.py` - Generate sample data
- `scripts/run_databricks_pipelines.py` - Execute all pipelines

### Configurations
- `configs/databricks/user_analytics_databricks.yaml`
- `configs/databricks/product_analytics_databricks.yaml`
- `configs/databricks/segment_analytics_databricks.yaml`

### Data
- `/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test/`
  - `user_events/` (Delta format)
  - `user_profiles/` (Delta format)
  - `products/` (Delta format)
  - `transactions/` (Delta format)

### Tables
- `nsangana_catalog.spark_pipeline_test.user_daily_metrics`
- `nsangana_catalog.spark_pipeline_test.product_performance_metrics`
- `nsangana_catalog.spark_pipeline_test.segment_performance_metrics`

## ✨ Key Achievements

1. ✅ Created realistic sample datasets in Databricks volume
2. ✅ Ran 3 production-like pipelines successfully
3. ✅ Created 3 analytics tables in Unity Catalog
4. ✅ Validated data quality at every step
5. ✅ Demonstrated conditional writes (write only if validation passes)
6. ✅ Used partitioning and optimization (Z-ordering)
7. ✅ Proved framework works end-to-end on Databricks

## 🎉 Conclusion

The Spark Pipeline Framework successfully:

- Reads data from Databricks volumes
- Performs complex multi-stage transformations
- Enforces data quality validation
- Writes to Unity Catalog tables
- Optimizes Delta tables automatically

**The framework is production-ready for real workloads!** 🚀

## 💡 Next Steps

1. **Explore the data**: Run the sample queries above
2. **Modify pipelines**: Edit YAML configs to add new transformations
3. **Add validations**: Add custom rules to ensure data quality
4. **Create new pipelines**: Use existing configs as templates
5. **Deploy to production**: Use these patterns for real datasets
