from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Random Forest") \
    .getOrCreate()

# Primer: Odobritev kredita
data = spark.createDataFrame([
    # starost, dohodek, zgodovina, odobren
    (21, 22000, "NE", 0),
    (23, 28000, "NE", 0),
    (25, 30000, "DA", 0),
    (27, 32000, "NE", 0),
    (29, 35000, "DA", 0),

    (30, 40000, "NE", 0),
    (32, 42000, "DA", 0),
    (34, 45000, "NE", 0),
    (36, 48000, "DA", 1),
    (38, 52000, "DA", 1),

    (40, 70000, "DA", 1),
    (42, 75000, "DA", 1),
    (44, 78000, "NE", 0),
    (45, 80000, "DA", 1),
    (47, 82000, "DA", 1),

    (48, 60000, "NE", 0),
    (50, 90000, "DA", 1),
    (52, 95000, "DA", 1),
    (55, 100000, "DA", 1),
    (58, 110000, "DA", 1),

    (60, 65000, "NE", 0),
    (62, 70000, "NE", 0),
    (65, 120000, "DA", 1),
    (68, 130000, "DA", 1),
    (70, 50000, "NE", 0)
], ["starost", "dohodek", "zgodovina", "odobren"])

# Kategorične feature-je v številke
indexer = StringIndexer(inputCol="zgodovina", outputCol="zgodovina_idx")
data = indexer.fit(data).transform(data)
data.show()
assembler = VectorAssembler(
    inputCols=["starost", "dohodek", "zgodovina_idx"],
    outputCol="features"
)
data = assembler.transform(data)
data.show()
train, test = data.randomSplit([0.85, 0.15], seed=55)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="odobren",
    numTrees=100,
    maxDepth=10
)
model = rf.fit(train)

predictions = model.transform(test)
predictions.select("starost", "dohodek", "zgodovina", "odobren", "prediction").show()

# Feature importance
print("Feature importance:")
for feature, importance in zip(["starost", "dohodek", "zgodovina"], model.featureImportances):
    print(f"  {feature}: {importance:.3f}")
