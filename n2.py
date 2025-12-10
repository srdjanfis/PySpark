from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, count, max as spark_max
import time

# Inicializacija Spark seje
spark = SparkSession.builder \
    .appName("PySpark Naloge") \
    .master("local[*]") \
    .getOrCreate()

print("\n" + "=" * 70)
print("NALOGA 2: Izbira in filtriranje podatkov")
print("=" * 70)

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

print("\n1c) Nalaganje orders.json:")

orders = spark.read.json("orders.json", multiLine=True)

# ============================================================================
# Naloga 2a: Izbira in preimenovanje stolpcev
# ============================================================================
print("\n2a) Izbira stolpcev in preimenovanje:")

products_selected = products.select(
    col("product_name").alias("naziv"),
    col("category"),
    col("price").alias("cena")
)

products_selected.show()

# ============================================================================
# Naloga 2b: Filtriranje izdelkov
# ============================================================================
print("\n2b) Filtriranje izdelkov:")

# Pogoj 1: Cena > 50 IN (kategorija Electronics ALI Books)
print("\nIzdelki s ceno > 50 EUR in kategorijo Electronics ali Books:")
filtered_products_1 = products.filter(
    (col("price") > 50) &
    ((col("category") == "Electronics") | (col("category") == "Books"))
)
filtered_products_1.show()

# Pogoj 2: Zaloga < 10
print("\nIzdelki z nizko zalogo (< 10):")
filtered_products_2 = products.filter(col("stock_quantity") < 10)
filtered_products_2.show()

# ============================================================================
# Naloga 2c: Izločanje stolpcev iz JSON-a
# ============================================================================
print("\n2c) Izločanje in preimenovanje stolpcev iz orders:")

orders_selected = orders.select(
    col("order_id"),
    col("customer.name").alias("ime_stranke"),
    col("customer.email").alias("email"),
    col("order_date"),
    col("total_amount")
)

orders_selected.show(truncate=False)

# ============================================================================
# Naloga 2d: Filtriranje naročil
# ============================================================================
print("\n2d) Filtriranje naročil (total_amount > 100 IN order_date > 2024-11-16):")

filtered_orders = orders_selected.filter(
    (col("total_amount") > 100) &
    (col("order_date") > "2024-11-16")
)

filtered_orders.show(truncate=False)