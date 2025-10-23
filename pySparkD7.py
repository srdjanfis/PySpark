
# ============================================================================
# EXERCISE 4: Filter data based on multiple conditions
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
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

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True)  # We'll parse this as date later
])

# Read with explicit schema
customers_explicit = spark.read \
    .option("header", "true") \
    .schema(customer_schema) \
    .csv("customers.csv")

print("\n" + "=" * 80)
print("EXERCISE 4: Filter data based on multiple conditions (age > 30 AND city = 'New York' or 'Chicago')")
print("=" * 80)

# Method 1: Using filter() with string expression
print("\n--- Method 1: filter() with string expression ---")
filtered_1 = customers_explicit.filter("age > 30 AND city IN ('New York', 'Chicago')")
filtered_1.show()

# Method 2: Using filter() with column expressions
print("\n--- Method 2: filter() with column expressions ---")
filtered_2 = customers_explicit.filter(
    (col("age") > 30) & (col("city").isin("New York", "Chicago"))
)
filtered_2.show()

# Method 3: Using where() (alias for filter)
print("\n--- Method 3: where() - same as filter ---")
filtered_3 = customers_explicit.where(
    (col("age") > 30) & (col("city").isin("New York", "Chicago"))
)
filtered_3.show()

# More complex filtering examples
print("\n--- More Complex Filters ---")

# Filter with OR condition
print("\n1. Age > 30 OR city = 'Phoenix':")
customers_explicit.filter(
    (col("age") > 30) | (col("city") == "Phoenix")
).show()

# Filter with multiple AND conditions
print("\n2. Age between 30 and 40 AND city starts with 'San':")
customers_explicit.filter(
    (col("age").between(30, 40)) & (col("city").startswith("San"))
).show()

# Filter with NOT condition
print("\n3. NOT from New York or Los Angeles:")
customers_explicit.filter(
    ~col("city").isin("New York", "Los Angeles")
).show()

# Important note about & vs and, | vs or
print("\n⚠️  IMPORTANT: Use & (bitwise AND) and | (bitwise OR), NOT 'and'/'or'")
print("Also, always use parentheses around each condition!")
