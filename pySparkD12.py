"""
PySpark Course - Sessions 3 & 4: Exercise Solutions
Session 3: Column Operations & Built-in Functions (4 exercises)
Session 4: Aggregations & GroupBy Operations (4 exercises)

Each session contains 4 carefully selected exercises covering the most important concepts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# ============================================================================
# SETUP: Create SparkSession
# ============================================================================

spark = SparkSession.builder \
    .appName("Sessions3-4-Solutions") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("SparkSession created successfully!")
print(f"Spark version: {spark.version}\n")


# ============================================================================
# SAMPLE DATA GENERATION
# ============================================================================

def generate_customers_data():
    """Generate customer data with messy/missing values"""
    data = [
        (1, "  John  ", "DOE", "1995-03-15", "john.doe@email.com", 1500.50, "New York"),
        (2, "jane", "SMITH", "1988-07-22", None, 2300.75, "Los Angeles"),
        (3, "  Bob", "johnson  ", None, "bob.j@email.com", None, "Chicago"),
        (4, "ALICE", "Williams", "1992-11-08", "alice.w@email.com", 980.00, None),
        (5, "Charlie", None, "1985-05-30", "charlie@email.com", 3200.00, "Phoenix"),
        (6, "  DIANA  ", "Davis", "1998-01-12", "diana.d@email.com", 1750.25, "Philadelphia"),
        (7, "Eve", "MILLER", "1990-09-18", None, 890.50, "San Antonio"),
        (8, "frank", "Wilson", "1987-12-25", "frank.w@email.com", 2100.00, "San Diego"),
    ]

    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("email", StringType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("city", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


def generate_transactions_data():
    """Generate transaction data for aggregations"""
    data = [
        (1, 1, "Electronics", "Laptop", 1200.00, "2024-01-15", "USA"),
        (2, 1, "Electronics", "Mouse", 25.50, "2024-01-16", "USA"),
        (3, 2, "Clothing", "Jacket", 89.99, "2024-01-15", "Canada"),
        (4, 3, "Electronics", "Headphones", 75.00, "2024-01-17", "USA"),
        (5, 2, "Books", "Python Guide", 45.00, "2024-01-18", "Canada"),
        (6, 4, "Electronics", "Headphones", 350.00, "2024-01-18", "UK"),
        (7, 1, "Books", "Data Science", 55.00, "2024-01-19", "USA"),
        (8, 5, "Clothing", "Shoes", 120.00, "2024-01-20", "USA"),
        (9, 3, "Electronics", "Headphones", 150.00, "2024-01-20", "USA"),
        (10, 2, "Electronics", "Headphones", 299.99, "2024-01-21", "Canada"),
        (11, 6, "Books", "Python Guide", 35.00, "2024-01-22", "UK"),
        (12, 4, "Clothing", "T-Shirt", 25.00, "2024-01-22", "UK"),
        (13, 1, "Electronics", "USB Cable", 12.50, "2024-01-23", "USA"),
        (14, 7, "Books", "Python Guide", 65.00, "2024-01-24", "Germany"),
        (15, 3, "Electronics", "Webcam", 89.00, "2024-01-25", "USA"),
    ]

    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("product", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("country", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)

# Generate data
print("Generating sample data...")
customers = generate_customers_data()
transactions = generate_transactions_data()
customers_clean = customers \
    .withColumn("first_name", F.trim(F.lower(F.col("first_name")))) \
    .withColumn("last_name", F.trim(F.lower(F.col("last_name")))) \
    .withColumn("email", F.lower(F.col("email")))
print("✓ Data generated\n")

print("\n--- Sample Transaction Data ---")
transactions.show()

# ============================================================================
# EXERCISE 2: Group by category and calculate statistics
# ============================================================================

print("\n" + "=" * 80)
print("EXERCISE 2: GroupBy with category - find top-selling categories")
print("=" * 80)

category_stats = transactions.groupBy("category") \
    .agg(
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_price"),
    F.count("transaction_id").alias("num_transactions"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.min("amount").alias("min_price"),
    F.max("amount").alias("max_price")
) \
    .orderBy(F.col("total_revenue").desc())

print("\n--- Revenue by Category ---")
category_stats.show()

# Round numeric columns for better readability
print("\n--- Formatted Category Stats ---")
category_stats.select(
    "category",
    F.round("total_revenue", 2).alias("total_revenue"),
    F.round("avg_price", 2).alias("avg_price"),
    "num_transactions",
    "unique_customers"
).show()

# Find most popular products per category
print("\n--- Most Popular Product per Category ---")
transactions.groupBy("category", "product") \
    .agg(
    F.count("transaction_id").alias("times_purchased"),
    F.sum("amount").alias("total_sales")
) \
    .orderBy("category", F.col("times_purchased").desc()) \
    .show()

print("\n💡 Key Takeaways:")
print("   - Use .groupBy() with one or more columns")
print("   - Chain .agg() after .groupBy() for aggregations")
print("   - Use .orderBy() to sort results")
print("   - Can group by multiple columns for hierarchical analysis")
