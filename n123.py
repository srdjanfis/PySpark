from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, count, max as spark_max
import time

# Inicijalizacija Spark sesije
spark = SparkSession.builder \
    .appName("PySpark Zadaci") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 70)
print("ZADATAK 1: Učitavanje i istraživanje podataka")
print("=" * 70)

# ============================================================================
# Zadatak 1a: Učitaj CSV sa automatskom detekcijom šeme
# ============================================================================
print("\n1a) Učitavanje products.csv sa automatskom detekcijom šeme:")
products_auto = spark.read.csv(
    "products.csv",
    header=True,
    inferSchema=True
)

print("\nPrvih 10 redova:")
products_auto.show(10)

print("\nAutomatski detektovana šema:")
products_auto.printSchema()

# ============================================================================
# Zadatak 1b: Ručno definiši šemu i učitaj
# ============================================================================
print("\n1b) Učitavanje products.csv sa ručno definisanom šemom:")

# Definisanje šeme
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

print("\nRučno definisana šema:")
products.printSchema()

print("\nPodaci sa ručno definisanom šemom:")
products.show()

# ============================================================================
# Zadatak 1c: Učitaj JSON sa ugnježdenom strukturom
# ============================================================================
print("\n1c) Učitavanje orders.json:")

orders = spark.read.json("orders.json")

print("\nPodaci iz JSON-a:")
orders.show(truncate=False)

# ============================================================================
# Zadatak 1d: Istraži strukturu i izdvoji ugnježdena polja
# ============================================================================
print("\n1d) Struktura JSON-a:")
orders.printSchema()

print("\nIzdvajanje ugnježdenih polja:")
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


print("\n" + "=" * 70)
print("ZADATAK 2: Selekcija i filtriranje podataka")
print("=" * 70)

# ============================================================================
# Zadatak 2a: Selekcija i preimenovanje kolona
# ============================================================================
print("\n2a) Selekcija kolona i preimenovanje:")

products_selected = products.select(
    col("product_name").alias("naziv"),
    col("category"),
    col("price").alias("cena")
)

products_selected.show()

# ============================================================================
# Zadatak 2b: Filtriranje proizvoda
# ============================================================================
print("\n2b) Filtriranje proizvoda:")

# Uslov 1: Cena > 50 I (kategorija Electronics ILI Books)
print("\nProizvodi sa cenom > 50 EUR i kategorijom Electronics ili Books:")
filtered_products_1 = products.filter(
    (col("price") > 50) &
    ((col("category") == "Electronics") | (col("category") == "Books"))
)
filtered_products_1.show()

# Uslov 2: Stock količina < 10
print("\nProizvodi sa niskim zalihama (< 10):")
filtered_products_2 = products.filter(col("stock_quantity") < 10)
filtered_products_2.show()

# ============================================================================
# Zadatak 2c: Izdvajanje kolona iz JSON-a
# ============================================================================
print("\n2c) Izdvajanje i preimenovanje kolona iz orders:")

orders_selected = orders.select(
    col("order_id"),
    col("customer.name").alias("customer_name"),
    col("customer.email").alias("email"),
    col("order_date"),
    col("total_amount")
)

orders_selected.show(truncate=False)

# ============================================================================
# Zadatak 2d: Filtriranje narudžbina
# ============================================================================
print("\n2d) Filtriranje narudžbina (total_amount > 100 I order_date > 2024-11-01):")

filtered_orders = orders_selected.filter(
    (col("total_amount") > 100) &
    (col("order_date") > "2024-11-01")
)

filtered_orders.show(truncate=False)


print("\n" + "=" * 70)
print("ZADATAK 3: Kombinovanje operacija i analiza plana izvršavanja")
print("=" * 70)

# ============================================================================
# Zadatak 3a: Kompleksan pipeline
# ============================================================================
print("\n3a) Kreiranje kompleksnog pipeline-a:")

# 1. Učitaj sa ručnom šemom (već imamo products DataFrame)
# 2. Filtriraj proizvode sa stock_quantity < 20
# 3. Selektuj određene kolone
# 4. Dodaj novu kolonu price_with_tax
# 5. Sortiraj po price_with_tax opadajuće
# 6. Grupisi po kategoriji

pipeline = products \
    .filter(col("stock_quantity") < 20) \
    .select("product_id", "product_name", "category", "price") \
    .withColumn("price_with_tax", col("price") * 1.22) \
    .orderBy(col("price_with_tax").desc())

print("\nProizvodi sa niskim zalihama, sortrani po ceni sa PDV-om:")
pipeline.show()

print("\nGrupiranje po kategoriji sa agregatnim funkcijama:")
aggregated = pipeline \
    .groupBy("category") \
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count"),
        spark_max("price").alias("max_price")
    )

aggregated.show()

# ============================================================================
# Zadatak 3b: Prikaz execution plan-a
# ============================================================================
print("\n3b) Logički plan (simple):")
print("-" * 70)
pipeline.explain(mode="simple")

print("\n\nFizički plan (extended):")
print("-" * 70)
pipeline.explain(mode="extended")

# ============================================================================
# Zadatak 3c: Dodavanje cache() i poređenje plana
# ============================================================================
print("\n\n3c) Dodavanje cache() i analiza plana:")

pipeline_cached = products \
    .filter(col("stock_quantity") < 20) \
    .cache() \
    .select("product_id", "product_name", "category", "price") \
    .withColumn("price_with_tax", col("price") * 1.22) \
    .orderBy(col("price_with_tax").desc())

print("\nPlan izvršavanja SA cache():")
print("-" * 70)
pipeline_cached.explain(mode="simple")

# ============================================================================
# Zadatak 3d: Poređenje performansi sa i bez keširanja
# ============================================================================
print("\n\n3d) Poređenje performansi sa i bez cache():")

# BEZ CACHE
print("\nBEZ CACHE:")
start_time = time.time()
pipeline.count()
time_no_cache_1 = time.time() - start_time
print(f"Prva akcija (count): {time_no_cache_1:.4f}s")

start_time = time.time()
pipeline.show()
time_no_cache_2 = time.time() - start_time
print(f"Druga akcija (show): {time_no_cache_2:.4f}s")

print(f"Ukupno vreme BEZ cache: {time_no_cache_1 + time_no_cache_2:.4f}s")

# SA CACHE
print("\nSA CACHE:")
start_time = time.time()
pipeline_cached.count()
time_cache_1 = time.time() - start_time
print(f"Prva akcija (count): {time_cache_1:.4f}s")

start_time = time.time()
pipeline_cached.show()
time_cache_2 = time.time() - start_time
print(f"Druga akcija (show): {time_cache_2:.4f}s")

print(f"Ukupno vreme SA cache: {time_cache_1 + time_cache_2:.4f}s")

print(f"\nUbrzanje: {((time_no_cache_1 + time_no_cache_2) / (time_cache_1 + time_cache_2)):.2f}x")

# ============================================================================
# Bonus: Objašnjenje transformacija vs akcija
# ============================================================================
print("\n\n" + "=" * 70)
print("BONUS: Transformacije vs Akcije")
print("=" * 70)

print("""
TRANSFORMACIJE (Lazy Evaluation):
- filter(), select(), withColumn(), orderBy(), groupBy() itd.
- NE izvršavaju se odmah
- Spark samo gradi execution plan (DAG)
- Vraćaju novi DataFrame

AKCIJE (Eager Execution):
- show(), count(), collect(), write() itd.
- POKREĆU izvršavanje celog pipeline-a
- Vraćaju rezultate ili side-effects

U execution plan-u možete videti:
1. Logički plan: Šta Spark planira da uradi (transformacije)
2. Fizički plan: Kako će to konkretno izvršiti (optimizovano)
3. InMemoryRelation: Ukazuje na kesrirane podatke

Cache() omogućava da se rezultat transformacija sačuva u memoriji,
pa se pri sledećoj akciji ne moraju ponovo računati sve transformacije.
""")

# Zaustavi Spark sesiju
spark.stop()