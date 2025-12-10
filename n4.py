from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round, trim, upper, when
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("Čiščenje podatkov in GroupBy - Rešitev") \
    .getOrCreate()

# Isti podatki kot zgoraj...
data = [
    (1, "Laptop", "Electronics", 1200.50, 2, "Ljubljana", "2024-01-15"),
    (2, "laptop", "electronics", 1200.50, 2, "Ljubljana", "2024-01-15"),
    (3, "Mouse", "Electronics", 25.00, 10, "Novo mesto", "2024-01-16"),
    (4, "Keyboard", "Electronics", None, 5, "Ljubljana", "2024-01-16"),
    (5, "Chair", "Furniture", 150.00, 3, "  Koper  ", "2024-01-17"),
    (6, "Desk", "Furniture", 300.00, 1, "Ljubljana", "2024-01-18"),
    (7, "Monitor", "Electronics", 250.00, 4, "Novo mesto", "2024-01-18"),
    (8, "MOUSE", "Electronics", 25.00, 8, "Koper", "2024-01-19"),
    (9, "Table", "Furniture", -50.00, 2, "Ljubljana", "2024-01-19"),
    (10, "", "Electronics", 100.00, 5, "Novo mesto", "2024-01-20"),
    (11, "Headphones", None, 75.00, 6, "Ljubljana", "2024-01-20"),
    (12, "Cable", "Electronics", 10.00, 0, "Koper", "2024-01-21"),
]

columns = ["id", "ime_izdelka", "kategorija", "cena", "količina", "mesto", "datum"]
df = spark.createDataFrame(data, columns)

print("=== IZVORNI PODATKI ===")
df.show()
print(f"Število vrstic: {df.count()}")

# ============================================
# REŠITEV
# ============================================

# Korak 1a: Odstranimo prazne 'ime_izdelka'
df_clean = df.filter(col("ime_izdelka") != "")

print("\n=== PO ODSTRANITVI PRAZNIH IMEN IZDELKOV ===")
df_clean.show()
print(f"Število vrstic: {df_clean.count()}")

# Korak 1b: Zapolnimo manjkajoče kategorije
df_clean = df_clean.fillna({"kategorija": "Neznano"})

print("\n=== PO ZAPOLNITVI PRAZNIH KATEGORIJ ===")
df_clean.show()

# Korak 1c: Izračunamo povprečno ceno po kategoriji (za zapolnitev None)
avg_prices = df_clean.filter(col("cena").isNotNull()) \
    .groupBy("kategorija") \
    .agg(avg("cena").alias("povp_cena"))

print("\n=== POVPREČNE CENE PO KATEGORIJAH ===")
avg_prices.show()

# Join in zapolnitev manjkajočih cen
df_clean = df_clean.join(avg_prices, "kategorija", "left")
df_clean = df_clean.withColumn(
    "cena",
    when(col("cena").isNull(), round(col("povp_cena"), 2))
    .otherwise(col("cena"))
).drop("povp_cena")

print("\n=== PO ZAPOLNJEVANJU CEN ===")
df_clean.show()

# Korak 1d: Čiščenje presledkov in pretvorba v velike črke
df_clean = df_clean.withColumn("mesto", trim(col("mesto")))
df_clean = df_clean.withColumn("ime_izdelka", upper(col("ime_izdelka")))
df_clean = df_clean.withColumn("kategorija", upper(col("kategorija")))

print("\n=== PO NORMALIZACIJI BESEDILA ===")
df_clean.show()

# Korak 1e: Odstranjevanje duplikatov
df_clean = df_clean.dropDuplicates(["ime_izdelka", "kategorija", "mesto"])

print("\n=== PO ODSTRANITVI DUPLIKATOV ===")
df_clean.show()
print(f"Število vrstic: {df_clean.count()}")

# Korak 1f: Odstranjevanje neveljavnih vrednosti (negativna cena, količina 0)
df_clean = df_clean.filter(
    (col("cena") > 0) & (col("količina") > 0)
)

print("\n=== PO ODSTRANITVI NEVELJAVNIH VREDNOSTI ===")
df_clean.show()
print(f"Število vrstic: {df_clean.count()}")

# ============================================
# Korak 2: GROUP BY ANALIZA
# ============================================

# Dodamo stolpec za prihodek
df_clean = df_clean.withColumn("prihodek", col("cena") * col("količina"))

# Group by kategorija + mesto
analysis = df_clean.groupBy("kategorija", "mesto").agg(
    round(sum("prihodek"), 2).alias("skupni_prihodek"),
    round(avg("cena"), 2).alias("povp_cena"),
    count("ime_izdelka").alias("št_izdelkov"),
    sum("količina").alias("skupna_količina")
)

print("\n=== ANALIZA PO KATEGORIJAH IN MESTIH ===")
analysis.show()

# Korak 3: Razvrščanje po prihodku
analysis_sorted = analysis.orderBy(col("skupni_prihodek").desc())

print("\n=== RAZVRŠČENO PO SKUPNEM PRIHODKU ===")
analysis_sorted.show()

# Korak 4: Filtriramo – samo prihodki > 1000
high_revenue = analysis_sorted.filter(col("skupni_prihodek") > 1000)

print("\n=== KOMBINACIJE S PRIHODKOM > 1000 ===")
high_revenue.show()

spark.stop()