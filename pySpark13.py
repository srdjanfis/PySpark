from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MapReduce WordCount") \
    .getOrCreate()

# Ustvarimo RDD z besedilom
text_data = [
    "Apache Spark je hiter in splošen pogon za obdelavo velikih podatkov",
    "Spark ponuja high-level API-je v Javi, Scali, Pythonu in R",
    "Spark prav tako podpira bogat nabor higher-level orodij",
    "PySpark je Python API za Apache Spark"
]

rdd = spark.sparkContext.parallelize(text_data)

print("=== IZVORNO BESEDILO ===")
for line in rdd.collect():
    print(line)

# ============================================
# MAP FAZA: Transformacija vsake vrstice
# ============================================
# 1. Razdelite vsako vrstico na besede
# 2. Preslikajte vsako besedo v par (beseda, 1)

words_rdd = rdd.flatMap(lambda line: line.lower().split())
print("\n=== PO flatMap (vse besede) ===")
print(words_rdd.collect())

# Map vsaki besedi dodeli vrednost 1
pairs_rdd = words_rdd.map(lambda word: (word, 1))
print("\n=== PO map (pari beseda-število) ===")
print(pairs_rdd.collect())

# ============================================
# REDUCE FAZA: Agregacija po ključu
# ============================================
# reduceByKey sešteje vse vrednosti za isti ključ

word_counts = pairs_rdd.reduceByKey(lambda a, b: a + b)
print("\n=== PO reduceByKey (štetje) ===")
print(word_counts.collect())

# Razvrščanje po frekvenci (padajoče)
sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)

print("\n=== KONČNI REZULTAT (razvrščeno) ===")
for word, count in sorted_counts.collect():
    print(f"{word}: {count}")

spark.stop()
