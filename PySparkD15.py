from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("MapReduce Dnevniki Strežnika") \
    .getOrCreate()

# Simulirani dnevniki strežnika
# Format: časovna oznaka, IP naslov, HTTP metoda, URL, statusna koda, čas odziva (ms)
logs_data = [
    "2024-01-15 10:23:45,192.168.1.100,GET,/home,200,45",
    "2024-01-15 10:24:12,192.168.1.101,POST,/login,200,120",
    "2024-01-15 10:25:33,192.168.1.100,GET,/products,200,67",
    "2024-01-15 10:26:45,192.168.1.102,GET,/home,404,12",
    "2024-01-15 10:27:22,192.168.1.100,GET,/checkout,500,3400",
    "2024-01-15 10:28:11,192.168.1.101,GET,/products,200,89",
    "2024-01-15 10:29:05,192.168.1.103,POST,/login,401,34",
    "2024-01-15 10:30:44,192.168.1.102,GET,/home,200,56",
    "2024-01-15 10:31:23,192.168.1.100,GET,/products,200,45",
    "2024-01-15 10:32:11,192.168.1.104,GET,/api/data,500,2100",
]

rdd = spark.sparkContext.parallelize(logs_data)

print("=== IZVIRNI DNEVNIKI ===")
for log in rdd.take(5):
    print(log)

# ============================================
# NALOGA 1: Število zahtev po IP naslovu
# ============================================

# MAP: Razčlenimo dnevnik in ekstrahiramo IP
ip_requests = rdd.map(lambda log: log.split(",")[1]) \
                .map(lambda ip: (ip, 1))

# REDUCE: Štejemo zahteve po IP
ip_counts = ip_requests.reduceByKey(lambda a, b: a + b)

print("\n=== ŠTEVILO ZAHTEV PO IP NASLOVU ===")
for ip, count in ip_counts.sortBy(lambda x: x[1], ascending=False).collect():
    print(f"{ip}: {count} zahtev")

# ============================================
# NALOGA 2: Povprečni čas odziva po URL-ju
# ============================================

# MAP: (URL, (čas_odziva, 1))
url_times = rdd.map(lambda log: log.split(",")) \
               .map(lambda parts: (parts[3], (int(parts[5]), 1)))

# REDUCE: (URL, (vsota_časa, število_zahtev))
url_totals = url_times.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Povprečje
url_averages = url_totals.mapValues(lambda x: round(x[0] / x[1], 2))

print("\n=== POVPREČNI ČAS ODZIVA PO URL-JU (ms) ===")
for url, avg_time in url_averages.sortBy(lambda x: x[1], ascending=False).collect():
    print(f"{url}: {avg_time} ms")

# ============================================
# NALOGA 3: Število napak po tipu (4xx, 5xx)
# ============================================

# MAP: Ekstrahiraj statusno kodo in kategoriziraj
def categorize_status(log):
    status = int(log.split(",")[4])
    if status >= 500:
        return ("5xx Napaka Strežnika", 1)
    elif status >= 400:
        return ("4xx Napaka Odjemalca", 1)
    elif status >= 200:
        return ("2xx Uspeh", 1)
    else:
        return ("Drugo", 1)

error_counts = rdd.map(categorize_status) \
                 .reduceByKey(lambda a, b: a + b)

print("\n=== PORAZDELITEV STATUSNIH KOD ===")
for category, count in error_counts.collect():
    print(f"{category}: {count}")

# ============================================
# NALOGA 4: Top 3 najpočasnejše končne točke (endpoint-i)
# ============================================

slow_endpoints = rdd.map(lambda log: log.split(",")) \
                   .map(lambda parts: (parts[3], int(parts[5]))) \
                   .sortBy(lambda x: x[1], ascending=False) \
                   .take(3)

print("\n=== TOP 3 NAJPOČASNEJŠE ZAHTEVE ===")
for url, time in slow_endpoints:
    print(f"{url}: {time} ms")

spark.stop()