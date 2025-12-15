from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt

# Inicializacija Spark
spark = SparkSession.builder \
    .appName("KMeans Naloga") \
    .master("local[*]") \
    .getOrCreate()

# Podatki
data = spark.createDataFrame([
    (25, 30000, 5),
    (45, 80000, 20),
    (30, 50000, 10),
    (50, 90000, 25),
    (28, 35000, 6),
    (42, 75000, 18),
    (35, 60000, 12)
], ["starost", "dohodek", "nakupi"])

print("Podatki:")
data.show()

# 1. CLUSTERING (k=3)
print("\n1. K-Means clustering (k=3)...")

# Priprava features
assembler = VectorAssembler(
    inputCols=["starost", "dohodek", "nakupi"],
    outputCol="features_raw"
)
data = assembler.transform(data)

# Normalizacija
scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
data = scaler.fit(data).transform(data)
data.show(truncate=False)

# K-Means model
kmeans = KMeans(k=2, seed=42)
model = kmeans.fit(data)

# Napovedi
predictions = model.transform(data)
predictions.select("starost", "dohodek", "nakupi", "prediction").show()

print("\nŠtevilo po klastrih:")
predictions.groupBy("prediction").count().orderBy("prediction").show()

print("\nPovprečne vrednosti po klastrih:")
predictions.groupBy("prediction").agg(
    {"starost": "avg", "dohodek": "avg", "nakupi": "avg"}
).orderBy("prediction").show()

# 2. VIZUALIZACIJA
print("\n2. Vizualizacija klastrov...")

df_pandas = predictions.select("starost", "dohodek", "nakupi", "prediction").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('K-Means Clustering (k=3)', fontsize=16, fontweight='bold')

colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']

# Graf 1: Starost vs Dohodek
for i in range(2):
    cluster = df_pandas[df_pandas['prediction'] == i]
    axes[0].scatter(cluster['starost'], cluster['dohodek'],
                    c=colors[i], label=f'Klaster {i}', s=150, alpha=0.7, edgecolors='black')

axes[0].set_xlabel('Starost', fontsize=12)
axes[0].set_ylabel('Dohodek', fontsize=12)
axes[0].set_title('Starost vs Dohodek', fontsize=13, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Graf 2: Nakupi vs Dohodek
for i in range(2):
    cluster = df_pandas[df_pandas['prediction'] == i]
    axes[1].scatter(cluster['nakupi'], cluster['dohodek'],
                    c=colors[i], label=f'Klaster {i}', s=150, alpha=0.7, edgecolors='black')

axes[1].set_xlabel('Število nakupov', fontsize=12)
axes[1].set_ylabel('Dohodek', fontsize=12)
axes[1].set_title('Nakupi vs Dohodek', fontsize=13, fontweight='bold')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('kmeans_clustering.png', dpi=150, bbox_inches='tight')
print("✓ Vizualizacija shranjena: kmeans_clustering.png")

plt.show()

spark.stop()