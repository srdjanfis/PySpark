from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Linearna regresija") \
    .getOrCreate()

# Primer: Napoved cene stanovanja glede na velikost
data = spark.createDataFrame([
    (50, 150000),
    (80, 220000),
    (100, 280000),
    (120, 320000),
    (150, 400000)
], ["velikost_m2", "cena"])

# Feature engineering: združi feature-je v vektor
assembler = VectorAssembler(
    inputCols=["velikost_m2"],
    outputCol="features"
)
data = assembler.transform(data)

data.show(truncate=False)

# Split na train/test
train, test = data.randomSplit([0.8, 0.2], seed=42)

# Treniraj model
lr = LinearRegression(featuresCol="features", labelCol="cena")
model = lr.fit(train)

# Napovedi
predictions = model.transform(test)
predictions.select("velikost_m2", "cena", "prediction").show()

# Evaluacija
print(f"Koeficient: {model.coefficients[0]:.2f}")
print(f"Intercept: {model.intercept:.2f}")
print(f"RMSE: {model.summary.rootMeanSquaredError:.2f}")
