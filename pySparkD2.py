from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from datetime import date
from pyspark.sql import Row
data = [
	Row(id = 1, value = 28.3, date = date(2021,1,1)),
	Row(id = 2, value = 15.8, date = date(2021,1,1)),
	Row(id = 3, value = 20.1, date = date(2021,1,2)),
	Row(id = 4, value = 12.6, date = date(2021,1,3))
]
df = spark.createDataFrame(data)
df.show()
data = [
	(12114, 'Anne', 21, 1.56, 8, 9, 10, 9, 'Economics', 'SC'),
    (13007, 'Adrian', 23, 1.82, 6, 6, 8, 7, 'Economics', 'SC'),
    (10045, 'George', 29, 1.77, 10, 9, 10, 7, 'Law', 'SC'),
    (12459, 'Adeline', 26, 1.61, 8, 6, 7, 7, 'Law', 'SC'),
    (10190, 'Mayla', 22, 1.67, 7, 7, 7, 9, 'Design', 'AR'),
    (11552, 'Daniel', 24, 1.75, 9, 9, 10, 9, 'Design', 'AR')
]
columns = [
	'StudentID', 'Name', 'Age', 'Height', 'Score1', 'Score2',
    'Score3', 'Score4', 'Course', 'Department'
]
students = spark.createDataFrame(data, columns)
students.explain()
students.show()