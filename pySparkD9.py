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
        (4, 3, "Electronics", "Keyboard", 75.00, "2024-01-17", "USA"),
        (5, 2, "Books", "Python Guide", 45.00, "2024-01-18", "Canada"),
        (6, 4, "Electronics", "Monitor", 350.00, "2024-01-18", "UK"),
        (7, 1, "Books", "Data Science", 55.00, "2024-01-19", "USA"),
        (8, 5, "Clothing", "Shoes", 120.00, "2024-01-20", "USA"),
        (9, 3, "Electronics", "Headphones", 150.00, "2024-01-20", "USA"),
        (10, 2, "Electronics", "Tablet", 299.99, "2024-01-21", "Canada"),
        (11, 6, "Books", "SQL Basics", 35.00, "2024-01-22", "UK"),
        (12, 4, "Clothing", "T-Shirt", 25.00, "2024-01-22", "UK"),
        (13, 1, "Electronics", "USB Cable", 12.50, "2024-01-23", "USA"),
        (14, 7, "Books", "Machine Learning", 65.00, "2024-01-24", "Germany"),
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
print("✓ Data generated\n")

# ============================================================================
# EXERCISE 1: Clean a messy dataset
# Handle NULLs, standardize text (lowercase), remove whitespace
# ============================================================================

print("\n" + "=" * 80)
print("EXERCISE 1: Clean messy dataset - NULLs, text standardization, whitespace")
print("=" * 80)

print("\n--- Original Data (with issues) ---")
customers.show(truncate=False)

print("\n--- Cleaning Steps ---")

# Step 1: Handle whitespace and standardize text case
customers_clean = customers \
    .withColumn("first_name", F.trim(F.lower(F.col("first_name")))) \
    .withColumn("last_name", F.trim(F.lower(F.col("last_name")))) \
    .withColumn("email", F.lower(F.col("email")))

print("\n1. After trimming whitespace and lowercasing:")
customers_clean.select("customer_id", "first_name", "last_name", "email").show()

# Step 2: Handle NULL values
# Option A: Fill NULLs with default values
customers_filled = customers_clean \
    .fillna({
    "last_name": "unknown",
    "email": "no-email@example.com",
    "birth_date": "1900-01-01",
    "purchase_amount": 0.0,
    "city": "Unknown"
})

print("\n2. After filling NULL values:")
customers_filled.show()

# Option B: Drop rows with ANY nulls
print("\n3. Alternative: Drop rows with NULL in critical columns:")
customers_dropped = customers_clean.dropna(subset=["email", "birth_date"])
customers_dropped.show()

# Step 3: Identify and flag NULL values before handling
print("\n4. Flagging NULL values for analysis:")
customers_with_flags = customers_clean \
    .withColumn("has_email", F.when(F.col("email").isNull(), "No").otherwise("Yes")) \
    .withColumn("has_birth_date", F.when(F.col("birth_date").isNull(), "No").otherwise("Yes"))

customers_with_flags.select("customer_id", "first_name", "has_email", "has_birth_date").show()

print("\n💡 Key Takeaways:")
print("   - Use F.trim() to remove whitespace")
print("   - Use F.lower() or F.upper() for standardization")
print("   - Use .fillna() to replace NULLs with defaults")
print("   - Use .dropna() to remove rows with NULLs")
print("   - Use F.isNull() and F.isNotNull() to identify NULLs")


