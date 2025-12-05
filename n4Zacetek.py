from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round, trim, upper, when
from pyspark.sql.types import DoubleType

# Inicializacija Spark seje
spark = SparkSession.builder \
    .appName("Data Cleaning and GroupBy") \
    .getOrCreate()

# Ustvarjanje DataFrame-a z "umazanimi" podatki
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

columns = ["id", "product_name", "category", "price", "quantity", "city", "date"]
df = spark.createDataFrame(data, columns)

print("=== ORIGINALNI PODACI ===")
df.show()

# ============================================
# NALOGA ZA ŠTUDENTE
# ============================================
#
# 1. ČIŠČENJE PODATKOV:
#    a) Odstranite vrstice, kjer je product_name prazen
#    b) Zapolnite manjkajoče cene (None) s povprečno ceno te kategorije
#    c) Zapolnite manjkajoče kategorije z "Unknown"
#    d) Odstranite dvojnike, ki temeljijo na product_name, category in city (ignorirajte velikost črk)
#    e) Odstranite vodilne/sledeče presledke v stolpcu city
#    f) Pretvorite product_name in category v VELIKE ČRKE
#    g) Odstranite vrstice, kjer je cena negativna ali je količina 0
#
# 2. ANALIZA (GROUP BY):
#    Po čiščenju izračunajte za vsako kombinacijo category + city:
#    - Skupni prihodek (price * quantity)
#    - Povprečno ceno izdelka
#    - Število različnih izdelkov
#    - Skupno količino prodanih izdelkov
#
# 3. Razvrstite rezultate po skupnem prihodku (padajoče)
#
# 4. Prikažite samo kombinacije, kjer je skupni prihodek > 1000
#
# ============================================

# VAŠA REŠITEV TUKAJ:
