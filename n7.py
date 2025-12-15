from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dec trees radnom forest") \
    .getOrCreate()

data = spark.createDataFrame([
    (36.5, 120, 70, 0),
    (38.5, 140, 95, 1),
    (37.0, 125, 75, 0),
    (39.0, 150, 100, 1),
    (36.8, 118, 68, 0),
    (38.0, 135, 90, 1),
    (37.2, 122, 72, 0),
    (39.5, 155, 105, 1)
], ["temp", "pritisk", "puls", "bolezen"])

assembler = VectorAssembler(
    inputCols=["temp", "pritisk", "puls"],
    outputCol="features"
)
data = assembler.transform(data)

train, test = data.randomSplit([0.8, 0.2], seed=42)

# Decision Tree
dt = DecisionTreeClassifier(featuresCol="features", labelCol="bolezen")
dt_model = dt.fit(train)
dt_pred = dt_model.transform(test)
dt_pred.show()

# Random Forest
rf = RandomForestClassifier(featuresCol="features", labelCol="bolezen", numTrees=20)
rf_model = rf.fit(train)
rf_pred = rf_model.transform(test)
rf_pred.show()
evaluator = MulticlassClassificationEvaluator(
    labelCol="bolezen",
    predictionCol="prediction",
    metricName="accuracy"
)

print(f"Decision Tree Accuracy: {evaluator.evaluate(dt_pred):.3f}")
print(f"Random Forest Accuracy: {evaluator.evaluate(rf_pred):.3f}")
