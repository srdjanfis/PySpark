
# ============================================================================
# EXERCISE 2: Load JSON data and explore nested structures
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

print("\n" + "=" * 80)
print("EXERCISE 2: Load JSON data and explore nested structures")
print("=" * 80)

# Read JSON data
posts = spark.read.json("social_posts.json")

print("\nJSON schema (note the nested structures):")
posts.printSchema()

print("\nAll posts:")
posts.show(truncate=False)

# Access nested fields
print("\n--- Accessing Nested Fields ---")

# Method 1: Using dot notation in selectExpr
posts.selectExpr(
    "post_id",
    "user.username",
    "user.verified",
    "engagement.likes",
    "content"
).show(truncate=False)

# Method 2: Using getField()
from pyspark.sql.functions import col

print("\nMethod 2: Using col().getField():")
posts.select(
    col("post_id"),
    col("user").getField("username").alias("username"),
    col("user").getField("verified").alias("verified"),
    col("engagement").getField("likes").alias("likes")
).show()

# Method 3: Using dot notation with col()
print("\nMethod 3: Using dot notation:")
posts.select(
    col("post_id"),
    col("user.username").alias("username"),
    col("engagement.likes").alias("likes"),
    col("engagement.shares").alias("shares"),
    col("engagement.comments").alias("comments")
).show()

