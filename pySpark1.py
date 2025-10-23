import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession
           .builder
           .appName("PySpark intro")
           .getOrCreate())

csv = "onlinefoods.csv"

df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csv))

df.show()
df.printSchema()