# ============================================================================
# EXERCISE 3: Select specific columns and rename them
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

print("\n" + "=" * 80)
print("EXERCISE 3: Select specific columns and rename them")
print("=" * 80)

# Method 1: Using select with alias
print("\n--- Method 1: select() with alias() ---")

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

customers_renamed_1 = customers_explicit.select(
    col("customer_id").alias("id"),
    col("first_name").alias("fname"),
    col("last_name").alias("lname"),
    col("age"),
    col("city")
)
customers_renamed_1.show()

# Method 2: Using selectExpr (more concise for multiple renames)
print("\n--- Method 2: selectExpr() ---")
customers_renamed_2 = customers_explicit.selectExpr(
    "customer_id as id",
    "first_name as fname",
    "last_name as lname",
    "age",
    "city"
)
customers_renamed_2.show()

# Method 3: Using withColumnRenamed (for single renames)
print("\n--- Method 3: withColumnRenamed() ---")
customers_renamed_3 = customers_explicit \
    .withColumnRenamed("customer_id", "id") \
    .withColumnRenamed("first_name", "fname") \
    .withColumnRenamed("last_name", "lname")
customers_renamed_3.show()

# Select subset of columns
print("\n--- Selecting subset of columns ---")
customers_subset = customers_explicit.select("first_name", "last_name", "city")
customers_subset.show()

# Using columns list
selected_cols = ["first_name", "city", "age"]
customers_explicit.select(selected_cols).show()
