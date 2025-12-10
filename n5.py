from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MapReduce Rešitev E-trgovina") \
    .getOrCreate()

transactions_data = [
    "T001,U101,Electronics,1200,1,Credit Card",
    "T002,U102,Clothing,45,2,PayPal",
    "T003,U101,Books,15,3,Credit Card",
    "T004,U103,Electronics,800,1,Debit Card",
    "T005,U102,Electronics,350,1,PayPal",
    "T006,U104,Clothing,60,1,Credit Card",
    "T007,U101,Electronics,150,2,Credit Card",
    "T008,U105,Books,25,4,Cash",
    "T009,U103,Clothing,80,1,Debit Card",
    "T010,U102,Books,30,2,PayPal",
    "T011,U104,Electronics,2200,1,Credit Card",
    "T012,U105,Clothing,55,3,Cash",
    "T013,U101,Books,12,5,Credit Card",
    "T014,U106,Electronics,450,1,PayPal",
    "T015,U103,Books,18,2,Debit Card",
]

rdd = spark.sparkContext.parallelize(transactions_data)

# ============================================
# REŠITEV
# ============================================

# Naloga 1: Skupni prihodek po kategoriji
print("\n=== NALOGA 1: Skupni prihodek po kategoriji ===")

category_revenue = rdd.map(lambda line: line.split(",")) \
                      .map(lambda parts: (parts[2], float(parts[3]) * int(parts[4]))) \
                      .reduceByKey(lambda a, b: a + b) \
                      .sortBy(lambda x: x[1], ascending=False)

for category, revenue in category_revenue.collect():
    print(f"{category}: ${revenue:.2f}")

# Naloga 2: Povprečna vrednost transakcije po načinu plačila
print("\n=== NALOGA 2: Povprečna vrednost transakcije po načinu plačila ===")

payment_avg = rdd.map(lambda line: line.split(",")) \
                 .map(lambda parts: (parts[5], (float(parts[3]) * int(parts[4]), 1))) \
                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                 .mapValues(lambda x: round(x[0] / x[1], 2)) \
                 .sortBy(lambda x: x[1], ascending=False)

for payment, avg_value in payment_avg.collect():
    print(f"{payment}: ${avg_value}")

# Naloga 3: Uporabnik z največjo porabo
print("\n=== NALOGA 3: Uporabnik z največjo porabo ===")

user_spending = rdd.map(lambda line: line.split(",")) \
                   .map(lambda parts: (parts[1], float(parts[3]) * int(parts[4]))) \
                   .reduceByKey(lambda a, b: a + b) \
                   .sortBy(lambda x: x[1], ascending=False)

print("\nVsi uporabniki (razvrščeno):")
for user, spending in user_spending.collect():
    print(f"{user}: ${spending:.2f}")

top_user = user_spending.first()
print(f"\nTop uporabnik: {top_user[0]} s skupno porabo ${top_user[1]:.2f}")

# Naloga 4: Skupno število prodanih artiklov po kategoriji
print("\n=== NALOGA 4: Skupno število prodanih artiklov po kategoriji ===")

items_per_category = rdd.map(lambda line: line.split(",")) \
                        .map(lambda parts: (parts[2], int(parts[4]))) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda x: x[1], ascending=False)

for category, items in items_per_category.collect():
    print(f"{category}: {items} artiklov")

# Naloga 5 (BONUS): Najdražja transakcija
print("\n=== NALOGA 5 (BONUS): Najdražja transakcija ===")

most_expensive = rdd.map(lambda line: line.split(",")) \
                     .map(lambda parts: (parts[0], float(parts[3]) * int(parts[4]))) \
                     .sortBy(lambda x: x[1], ascending=False) \
                     .first()

print(f"Najdražja transakcija: {most_expensive[0]} - ${most_expensive[1]:.2f}")

# DODATNA ANALIZA: Top 3 transakcije
print("\n=== Top 3 transakcije po vrednosti ===")
top_3 = rdd.map(lambda line: line.split(",")) \
           .map(lambda parts: (parts[0], float(parts[3]) * int(parts[4]))) \
           .sortBy(lambda x: x[1], ascending=False) \
           .take(3)

for trans_id, value in top_3:
    print(f"{trans_id}: ${value:.2f}")