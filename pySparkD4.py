# ============================================================================
# EXERCISE 1: Read CSV with schema inference, then define explicit schema
# ============================================================================

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

print("=" * 80)
print("EXERCISE 1: Read a CSV file with schema inference, then define explicit schema")
print("=" * 80)

# Part A: Read with schema inference
print("\n--- Part A: Schema Inference ---")
customers_inferred = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("customers.csv")

print("Schema with inference:")
customers_inferred.printSchema()

print("\nFirst 5 rows:")
customers_inferred.show(5)

# Part B: Define explicit schema
print("\n--- Part B: Explicit Schema Definition ---")

# Define the schema explicitly
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

print("Schema with explicit definition:")
customers_explicit.printSchema()

print("\nKey differences:")
print("- Explicit schema is faster (no need to scan data)")
print("- Explicit schema ensures correct data types")
print("- Explicit schema allows nullable constraints")
print("- In production, ALWAYS use explicit schemas for reliability")