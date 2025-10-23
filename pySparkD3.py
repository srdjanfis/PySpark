"""
PySpark Course - Session 2: Reading Data & Basic DataFrame Operations
Exercise Solutions with Sample Data Generation

This file contains complete solutions for all 5 exercises in Session 2.
Students should work through exercises sequentially.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import json
from datetime import datetime, timedelta
import random

# ============================================================================
# SETUP: Create SparkSession
# ============================================================================

spark = SparkSession.builder \
    .appName("Session2-Solutions") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("SparkSession created successfully!")
print(f"Spark version: {spark.version}\n")


# ============================================================================
# SAMPLE DATA GENERATION (Students can use this or download real datasets)
# ============================================================================

def generate_sample_csv_data():
    """Generate sample customer CSV data"""
    csv_data = """customer_id,first_name,last_name,age,city,signup_date
1,John,Doe,28,New York,2023-01-15
2,Jane,Smith,34,Los Angeles,2023-02-20
3,Bob,Johnson,45,Chicago,2023-03-10
4,Alice,Williams,29,Houston,2023-04-05
5,Charlie,Brown,52,Phoenix,2023-05-12
6,Diana,Davis,31,Philadelphia,2023-06-18
7,Eve,Miller,26,San Antonio,2023-07-22
8,Frank,Wilson,38,San Diego,2023-08-30
9,Grace,Moore,41,Dallas,2023-09-14
10,Henry,Taylor,33,San Jose,2023-10-25"""

    with open('customers.csv', 'w') as f:
        f.write(csv_data)
    print("✓ Created customers.csv")


def generate_sample_json_data():
    """Generate sample social media posts JSON data"""
    posts = [
        {
            "post_id": 1,
            "user": {"user_id": 101, "username": "tech_guru", "verified": True},
            "content": "Just learned PySpark! Amazing for big data.",
            "timestamp": "2024-01-15T10:30:00",
            "engagement": {"likes": 45, "shares": 12, "comments": 8}
        },
        {
            "post_id": 2,
            "user": {"user_id": 102, "username": "data_scientist", "verified": False},
            "content": "Working on a machine learning project with Spark MLlib",
            "timestamp": "2024-01-16T14:20:00",
            "engagement": {"likes": 89, "shares": 23, "comments": 15}
        },
        {
            "post_id": 3,
            "user": {"user_id": 103, "username": "code_ninja", "verified": True},
            "content": "DataFrames in PySpark are so much faster than Pandas for large datasets",
            "timestamp": "2024-01-17T09:15:00",
            "engagement": {"likes": 134, "shares": 45, "comments": 28}
        },
        {
            "post_id": 4,
            "user": {"user_id": 104, "username": "analytics_pro", "verified": False},
            "content": "Exploring window functions today. Mind blown!",
            "timestamp": "2024-01-18T16:45:00",
            "engagement": {"likes": 67, "shares": 18, "comments": 11}
        },
        {
            "post_id": 5,
            "user": {"user_id": 105, "username": "spark_enthusiast", "verified": True},
            "content": "Built my first PySpark ETL pipeline. Production ready!",
            "timestamp": "2024-01-19T11:30:00",
            "engagement": {"likes": 156, "shares": 67, "comments": 34}
        }
    ]

    with open('social_posts.json', 'w') as f:
        for post in posts:
            f.write(json.dumps(post) + '\n')
    print("✓ Created social_posts.json")


# Generate sample data files
print("Generating sample data files...")
generate_sample_csv_data()
generate_sample_json_data()
print()


