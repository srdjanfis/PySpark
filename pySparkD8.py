
# ============================================================================
# EXERCISE 5: Chain multiple operations and observe the execution plan
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
print("EXERCISE 5: Chain multiple operations and observe the execution plan")
print("=" * 80)

print("\n--- Chaining Operations ---")

# Build a complex query by chaining operations
result = customers_explicit \
    .filter(col("age") >= 30) \
    .filter(col("city").isin("New York", "Los Angeles", "Chicago", "Houston")) \
    .select(
    col("customer_id").alias("id"),
    col("first_name"),
    col("last_name"),
    col("age"),
    col("city")
) \
    .orderBy(col("age").desc())

# Show the result
print("\nResult of chained operations:")
result.show()

# View the logical plan (before optimization)
print("\n--- Logical Plan (before optimization) ---")
print(result.explain(extended=False))

# View the physical plan (after optimization)
print("\n--- Physical Plan (with optimization) ---")
result.explain(mode="formatted")

# Count records at different stages to understand lazy evaluation
print("\n--- Understanding Lazy Evaluation ---")
print("Creating transformations (no execution yet)...")

step1 = customers_explicit.filter(col("age") >= 30)
print("✓ Step 1: Filtered by age >= 30")

step2 = step1.filter(col("city").isin("New York", "Los Angeles", "Chicago"))
print("✓ Step 2: Filtered by city")

step3 = step2.select("first_name", "last_name", "city")
print("✓ Step 3: Selected columns")

print("\nNo data has been processed yet! (Lazy evaluation)")
print("Now calling an ACTION (.count()) to trigger execution...")

count = step3.count()
print(f"\nFinal count: {count} records")
print("Now the entire pipeline was executed!")

# Compare execution plans
print("\n--- Comparing Execution Plans ---")
print("\nSimple query plan:")
customers_explicit.select("first_name", "city").explain()

print("\nComplex chained query plan:")
result.explain()

# Demonstrate predicate pushdown optimization
print("\n--- Demonstrating Predicate Pushdown ---")
print("When reading from data sources like Parquet, filters are 'pushed down'")
print("This means filtering happens at the source, reducing data transfer!")
