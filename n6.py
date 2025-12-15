from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Napoved plače") \
    .getOrCreate()

data = spark.createDataFrame([
    (2, 12, 25000),
    (5, 16, 45000),
    (10, 18, 70000),
    (1, 12, 22000),
    (15, 20, 95000),
    (8, 16, 60000),
    (3, 14, 35000)
], ["leta_izk", "izobrazba", "placa"])


assembler = VectorAssembler(
    inputCols=["leta_izk", "izobrazba"],
    outputCol="features"
)
data = assembler.transform(data)
data.show()
train, test = data.randomSplit([0.80, 0.2], seed=123)

lr = LinearRegression(featuresCol="features", labelCol="placa")
model = lr.fit(train)

predictions = model.transform(test)
predictions.select("leta_izk", "izobrazba", "placa", "prediction").show()

evaluator = RegressionEvaluator(
    labelCol="placa",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.2f}")
print(f"R²: {model.summary.r2:.3f}")