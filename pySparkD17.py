from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Logistična regresija") \
    .getOrCreate()

# Primer: Ali bo študent opravil izpit?
data = spark.createDataFrame([
    (5, 80, 1),  # ure_študija, test_točke, opravil
    (2, 45, 0),
    (8, 90, 1),
    (1, 35, 0),
    (6, 75, 1),
    (3, 50, 0),
    (9, 95, 1)
], ["ure", "tocke", "opravil"])

assembler = VectorAssembler(
    inputCols=["ure", "tocke"],
    outputCol="features"
)
data = assembler.transform(data)

train, test = data.randomSplit([0.7, 0.3], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="opravil")
model = lr.fit(train)

predictions = model.transform(test)
predictions.select("ure", "tocke", "opravil", "prediction", "probability").show()

# Evaluacija
evaluator = BinaryClassificationEvaluator(labelCol="opravil")
auc = evaluator.evaluate(predictions)
print(f"AUC: {auc:.3f}")
