from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, count, max as spark_max
import time

# Inicijalizacija Spark sesije
spark = SparkSession.builder \
    .appName("PySpark Naloge") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 70)
print("NALOGA 1: Nalaganje in raziskava podatkov")
print("=" * 70)

# ============================================================================
# Naloga 1a: Nalaganje products.csv sa samodejno zaznano shemo
# ============================================================================
print("\n1a) Nalaganje products.csv sa samodejno zaznano shemo:")
products_auto = spark.read.csv(
    "products.csv",
    header=True,
    inferSchema=True
)

print("\nPrvih 10 vrstic:")
products_auto.show(10)

print("\nSamodejno zaznana shema:")
products_auto.printSchema()

# ============================================================================
# Naloga 1b: Nalaganje products.csv sa ročno določeno šemo
# ============================================================================
print("\n1b) Nalaganje products.csv sa ročno določeno šemo:")

products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("stock_quantity", IntegerType(), nullable=False)
])

products = spark.read.csv(
    "products.csv",
    header=True,
    schema=products_schema
)

print("\nRočno določena šema:")
products.printSchema()

print("\nPodatki z ročno določeno šemo:")
products.show()

# ============================================================================
# Naloga 1c: Naloži JSON s ugnezdeno strukturo
# ============================================================================
print("\n1c) Nalaganje orders.json:")

orders = spark.read.json("orders.json")

print("\nPodatki iz JSON-a:")
orders.show(truncate=False)

# ============================================================================
# Naloga 1d: Struktura JSON-a
# ============================================================================
print("\n1d) Struktura JSON-a:")
orders.printSchema()

print("\nOznačevanje vgnezdenih polj:")
orders_flat = orders.select(
    col("order_id"),
    col("customer.customer_id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("customer.email").alias("customer_email"),
    col("order_date"),
    col("total_amount"),
    col("items")
)

orders_flat.show(truncate=False)