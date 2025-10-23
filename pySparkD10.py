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
customers_clean = customers \
    .withColumn("first_name", F.trim(F.lower(F.col("first_name")))) \
    .withColumn("last_name", F.trim(F.lower(F.col("last_name")))) \
    .withColumn("email", F.lower(F.col("email")))
print("✓ Data generated\n")

# ============================================================================
# EXERCISE 2: Calculate age from birth_date and create full_name
# ============================================================================

print("\n" + "=" * 80)
print("EXERCISE 2: Calculate age from birth_date and create full_name column")
print("=" * 80)

# First, convert string date to proper date type
customers_with_dates = customers_clean \
    .withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))

# Calculate age
customers_with_age = customers_with_dates \
    .withColumn("age",
                F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
                )

# Create full_name by concatenating first and last name
customers_with_fullname = customers_with_age \
    .withColumn("full_name",
                F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
                )

print("\n--- Result with age and full_name ---")
customers_with_fullname.select(
    "customer_id",
    "full_name",
    "birth_date",
    "age",
    "email"
).show()

# Alternative: Using concat with explicit space
print("\n--- Alternative concat method ---")
customers.select(
    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name_v2")
).show(5)

print("\n💡 Key Takeaways:")
print("   - Use F.to_date() to convert string to date")
print("   - Use F.datediff() and F.current_date() for date arithmetic")
print("   - Use F.concat_ws() for concatenation with separator")
print("   - Use F.lit() to add literal values")

