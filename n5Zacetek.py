from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MapReduce E-trgovina Naloga") \
    .getOrCreate()

# E-trgovina transakcije
# Format: ID_transakcije, ID_uporabnika, kategorija_izdelka, cena_izdelka, količina, način_plačila
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

print("=== E-TRGOVINA TRANSAKCIJE ===")
for transaction in rdd.take(5):
    print(transaction)

# ============================================
# NALOGE ZA ŠTUDENTE (uporabite pristop MapReduce)
# ============================================
#
# 1. Izračunajte SKUPNI PRIHODEK po kategoriji izdelka
#    (cena * količina)
#    Izhod: (kategorija, skupni_prihodek)
#
# 2. Izračunajte POVPREČNO VREDNOST TRANSAKCIJE po načinu plačila
#    (povprečje cena * količina)
#    Izhod: (način_plačila, povprečna_vrednost_transakcije)
#
# 3. Poiščite uporabnika (ID_uporabnika) z NAJVEČJO SKUPNO PORABO
#    Izhod: (ID_uporabnika, skupna_poraba)
#
# 4. Izračunajte SKUPNO ŠTEVILO PRODANIH ARTIKLOV po kategoriji
#    (vsota količine)
#    Izhod: (kategorija, skupno_število_prodanih_artiklov)
#
# 5. BONUS: Poiščite najdražjo posamezno transakcijo
#    Izhod: (ID_transakcije, skupna_vrednost)
#
# OPOMBA: Uporabite map(), flatMap(), reduceByKey(), sortBy()
# ============================================

# VAŠA REŠITEV TUKAJ:

# Naloga 1: Skupni prihodek po kategoriji








# Naloga 2: Povprečna vrednost transakcije po načinu plačila








# Naloga 3: Uporabnik z največjo porabo








# Naloga 4: Skupno število prodanih artiklov po kategoriji








# Naloga 5 (BONUS): Najdražja transakcija








# ============================================
# KONEC NALOGE
# ============================================

spark.stop()