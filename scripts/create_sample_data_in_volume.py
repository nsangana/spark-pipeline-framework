#!/usr/bin/env python3
"""
Create sample data in Databricks volume for testing.

This script creates sample datasets in the Databricks volume:
/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test

Datasets created:
1. user_events - User activity events (page views, clicks, purchases)
2. user_profiles - User profile information
3. products - Product catalog
4. transactions - Purchase transactions
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint, uniform

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from databricks.connect import DatabricksSession


def create_user_events_data(spark, base_path: str, num_events: int = 1000):
    """Create user events dataset.

    Args:
        spark: Databricks SparkSession
        base_path: Base path in volume
        num_events: Number of events to generate
    """
    print(f"\n📊 Generating {num_events} user events...")

    event_types = ["page_view", "click", "search", "add_to_cart", "purchase"]
    user_ids = list(range(1, 101))  # 100 users
    product_ids = list(range(1, 51))  # 50 products

    events = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_events):
        event_id = i + 1
        user_id = choice(user_ids)
        event_type = choice(event_types)
        product_id = choice(product_ids) if event_type in ["click", "add_to_cart", "purchase"] else None

        # Generate timestamp within last 30 days
        timestamp = base_date + timedelta(
            days=randint(0, 29),
            hours=randint(0, 23),
            minutes=randint(0, 59),
            seconds=randint(0, 59)
        )

        # Add value for purchase events
        revenue = round(uniform(10, 500), 2) if event_type == "purchase" else None

        events.append((
            event_id,
            user_id,
            event_type,
            product_id,
            timestamp,
            revenue
        ))

    # Create DataFrame
    schema = ["event_id", "user_id", "event_type", "product_id", "timestamp", "revenue"]
    events_df = spark.createDataFrame(events, schema)

    # Write to volume as Delta
    output_path = f"{base_path}/user_events"
    events_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"✓ Created user_events with {events_df.count()} rows at {output_path}")
    return events_df


def create_user_profiles_data(spark, base_path: str):
    """Create user profiles dataset.

    Args:
        spark: Databricks SparkSession
        base_path: Base path in volume
    """
    print(f"\n👤 Generating user profiles...")

    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    segments = ["free", "premium", "enterprise"]
    countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX"]

    profiles = []
    base_signup_date = datetime(2023, 1, 1)

    for user_id in range(1, 101):  # 100 users
        first_name = choice(first_names)
        last_name = choice(last_names)
        full_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{user_id}@example.com"
        age = randint(18, 65)
        segment = choice(segments)
        country = choice(countries)

        # Signup date within last 2 years
        signup_date = (base_signup_date + timedelta(days=randint(0, 730))).date()

        profiles.append((
            user_id,
            full_name,
            email,
            age,
            segment,
            country,
            str(signup_date)
        ))

    # Create DataFrame
    schema = ["user_id", "name", "email", "age", "segment", "country", "signup_date"]
    profiles_df = spark.createDataFrame(profiles, schema)

    # Write to volume as Delta
    output_path = f"{base_path}/user_profiles"
    profiles_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"✓ Created user_profiles with {profiles_df.count()} rows at {output_path}")
    return profiles_df


def create_products_data(spark, base_path: str):
    """Create products dataset.

    Args:
        spark: Databricks SparkSession
        base_path: Base path in volume
    """
    print(f"\n🛍️  Generating products...")

    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys", "Food", "Beauty"]
    adjectives = ["Premium", "Essential", "Deluxe", "Basic", "Pro", "Ultra", "Mini", "Mega"]
    nouns = ["Widget", "Gadget", "Tool", "Kit", "Set", "Bundle", "Pack", "Collection"]

    products = []

    for product_id in range(1, 51):  # 50 products
        category = choice(categories)
        name = f"{choice(adjectives)} {choice(nouns)} {product_id}"
        price = round(uniform(9.99, 999.99), 2)
        stock = randint(0, 1000)
        rating = round(uniform(3.0, 5.0), 1)

        products.append((
            product_id,
            name,
            category,
            price,
            stock,
            rating
        ))

    # Create DataFrame
    schema = ["product_id", "product_name", "category", "price", "stock_quantity", "rating"]
    products_df = spark.createDataFrame(products, schema)

    # Write to volume as Delta
    output_path = f"{base_path}/products"
    products_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"✓ Created products with {products_df.count()} rows at {output_path}")
    return products_df


def create_transactions_data(spark, base_path: str, num_transactions: int = 500):
    """Create transactions dataset.

    Args:
        spark: Databricks SparkSession
        base_path: Base path in volume
        num_transactions: Number of transactions to generate
    """
    print(f"\n💳 Generating {num_transactions} transactions...")

    user_ids = list(range(1, 101))
    product_ids = list(range(1, 51))
    payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
    statuses = ["completed", "pending", "cancelled", "refunded"]

    transactions = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_transactions):
        transaction_id = i + 1
        user_id = choice(user_ids)
        product_id = choice(product_ids)
        quantity = randint(1, 5)
        unit_price = round(uniform(9.99, 999.99), 2)
        total_amount = round(unit_price * quantity, 2)
        payment_method = choice(payment_methods)
        status = choice(statuses) if randint(1, 100) > 95 else "completed"  # 95% completed

        # Generate timestamp within last 30 days
        transaction_date = base_date + timedelta(
            days=randint(0, 29),
            hours=randint(0, 23),
            minutes=randint(0, 59)
        )

        transactions.append((
            transaction_id,
            user_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            payment_method,
            status,
            transaction_date
        ))

    # Create DataFrame
    schema = [
        "transaction_id", "user_id", "product_id", "quantity",
        "unit_price", "total_amount", "payment_method", "status", "transaction_date"
    ]
    transactions_df = spark.createDataFrame(transactions, schema)

    # Write to volume as Delta
    output_path = f"{base_path}/transactions"
    transactions_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"✓ Created transactions with {transactions_df.count()} rows at {output_path}")
    return transactions_df


def main():
    """Main function."""
    print("=" * 70)
    print("Creating Sample Data in Databricks Volume")
    print("=" * 70)

    # Configuration
    volume_path = "/Volumes/nsangana_catalog/spark_pipeline_test/databricks_unit_test"
    profile = "spark-pipeline-testing"

    print(f"\n📁 Target volume: {volume_path}")
    print(f"🔐 Using profile: {profile}")

    # Create Databricks session
    print(f"\n🔌 Connecting to Databricks...")
    spark = DatabricksSession.builder.profile(profile).getOrCreate()
    print(f"✓ Connected to Databricks (Spark {spark.version})")

    try:
        # Create all datasets
        create_user_events_data(spark, volume_path, num_events=1000)
        create_user_profiles_data(spark, volume_path)
        create_products_data(spark, volume_path)
        create_transactions_data(spark, volume_path, num_transactions=500)

        print("\n" + "=" * 70)
        print("✅ SUCCESS: All sample data created!")
        print("=" * 70)

        print(f"\n📊 Datasets created in {volume_path}:")
        print(f"  1. user_events      - 1,000 rows (user activity)")
        print(f"  2. user_profiles    - 100 rows (user info)")
        print(f"  3. products         - 50 rows (product catalog)")
        print(f"  4. transactions     - 500 rows (purchases)")

        print(f"\n💡 Next steps:")
        print(f"  1. Run pipelines with: python scripts/run_databricks_pipelines.py")
        print(f"  2. Or test individual pipelines from configs/databricks/")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
