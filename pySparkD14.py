from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MapReduce Povprečne Ocene") \
    .getOrCreate()

# Podatki: (študent, predmet, ocena)
grades_data = [
    ("Ana", "Matematika", 85),
    ("Marko", "Matematika", 90),
    ("Ana", "Fizika", 78),
    ("Jovana", "Matematika", 92),
    ("Marko", "Fizika", 88),
    ("Ana", "Kemija", 95),
    ("Jovana", "Fizika", 85),
    ("Marko", "Kemija", 80),
    ("Jovana", "Kemija", 88),
]

rdd = spark.sparkContext.parallelize(grades_data)

print("=== IZVIRNI PODATKI ===")
for record in rdd.collect():
    print(record)

# ============================================
# CILJ: Izračunati povprečno oceno po predmetu
# ============================================

# MAP FAZA: Ekstrahiramo predmet kot ključ in oceno kot vrednost
# Format: (predmet, ocena)
subject_grades = rdd.map(lambda x: (x[1], x[2]))

print("\n=== MAP: (predmet, ocena) ===")
print(subject_grades.collect())

# PROBLEM: reduceByKey lahko samo sešteva, ne more neposredno izračunati povprečja
# REŠITEV: Mapiramo na (predmet, (ocena, 1)), da shranimo tudi število ocen

subject_grade_counts = subject_grades.map(lambda x: (x[0], (x[1], 1)))

print("\n=== MAP: (predmet, (ocena, število)) ===")
print(subject_grade_counts.collect())

# REDUCE FAZA: Seštejemo ocene in števila
# (vsota_ocen, število_ocen)
totals = subject_grade_counts.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

print("\n=== REDUCE: (predmet, (vsota_ocen, število_ocen)) ===")
print(totals.collect())

# Končno: Izračun povprečja
averages = totals.mapValues(lambda x: round(x[0] / x[1], 2))

print("\n=== POVPREČNE OCENE PO PREDMETU ===")
for subject, avg in averages.collect():
    print(f"{subject}: {avg}")

# Bonus: Poišči predmet z najvišjo povprečno oceno
best_subject = averages.sortBy(lambda x: x[1], ascending=False).first()
print(f"\nNajboljši predmet: {best_subject[0]} (povprečje: {best_subject[1]})")

spark.stop()