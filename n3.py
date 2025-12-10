from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, count, max as spark_max
import time

# Inicializacija Spark seje
spark = SparkSession.builder \
    .appName("PySpark Naloge") \
    .master("local[*]") \
    .getOrCreate()

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

print("\n" + "=" * 70)
print("NALOGA 3: Kombiniranje operacij in analiza izvršitvenega načrta")
print("=" * 70)

# ============================================================================
# Naloga 3a: Kompleksen pipeline
# ============================================================================
print("\n3a) Ustvarjanje kompleksnega pipeline-a:")

# 1. Naloži s podano shemo (products DataFrame že imamo)
# 2. Filtriraj izdelke z zalogo stock_quantity < 20
# 3. Izberi določene stolpce
# 4. Dodaj nov stolpec price_with_tax
# 5. Uredi po price_with_tax padajoče

pipeline = products \
    .filter(col("stock_quantity") < 20) \
    .select("product_id", "product_name", "category", "price") \
    .withColumn("price_with_tax", col("price") * 1.22) \
    .orderBy(col("price_with_tax").desc())

print("\nIzdelki z nizko zalogo, urejeni po ceni s DDV-jem:")
pipeline.show()

print("\nGrupiranje po kategoriji z agregatnimi funkcijami:")
aggregated = pipeline \
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count"),
        spark_max("price").alias("max_price")
    )

aggregated.show()

# ============================================================================
# Naloga 3b: Prikaz izvršitvenega načrta (execution plan)
# ============================================================================
print("\n3b) Logični načrt (simple):")
print("-" * 70)
pipeline.explain(mode="simple")

print("\n\nFizični načrt (extended):")
print("-" * 70)
pipeline.explain(mode="extended")

spark.stop()