"""
PySpark K-Means Clustering - Kratak primer
==========================================
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.ml.clustering import KMeans

# Inicijalizacija Spark
spark = SparkSession.builder \
    .appName("KMeans Example") \
    .master("local[*]") \
    .getOrCreate()

# Kreiranje podataka
data = [
    (1, 25, 30000, 200),
    (2, 45, 80000, 800),
    (3, 35, 50000, 450),
    (4, 52, 95000, 1200),
    (5, 23, 28000, 150),
    (6, 38, 62000, 600),
    (7, 48, 88000, 950),
    (8, 29, 35000, 280),
    (9, 55, 105000, 1500),
    (10, 31, 45000, 380),
]

df = spark.createDataFrame(data, ["id", "age", "income", "spending"])
df.show()

# Priprema features
assembler = VectorAssembler(
    inputCols=["age", "income", "spending"],
    outputCol="features_raw"
)
df = assembler.transform(df)

# Normalizacija
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
df = scaler.fit(df).transform(df)

# K-Means
kmeans = KMeans(k=3, seed=42)
model = kmeans.fit(df)

# Predikcije
predictions = model.transform(df)
predictions.select("id", "age", "income", "spending", "prediction").show()

# Rezultati
print(f"\nŠtevilo objektov v gručah:")
predictions.groupBy("prediction").count().show()

print(f"\nPovprečne vrednosti:")
predictions.groupBy("prediction").agg(
    {"age": "avg", "income": "avg", "spending": "avg"}
).show()

# Vizualizacija
import matplotlib.pyplot as plt

df_pandas = predictions.select("age", "income", "prediction").toPandas()

plt.figure(figsize=(10, 6))
colors = ['red', 'blue', 'green']
for i in range(3):
    cluster = df_pandas[df_pandas['prediction'] == i]
    plt.scatter(cluster['age'], cluster['income'],
                c=colors[i], label=f'Klaster {i}', s=100, alpha=0.6)

plt.xlabel('Starost', fontsize=12)
plt.ylabel('Dohodek', fontsize=12)
plt.title('K-Means Clustering', fontsize=14, fontweight='bold')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig('kmeans_result.png', dpi=150, bbox_inches='tight')
print("\n✓ Vizualizacija shranjena v: kmeans_result.png")

spark.stop()