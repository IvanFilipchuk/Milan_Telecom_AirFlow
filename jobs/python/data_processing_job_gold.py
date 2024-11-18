from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

# Inicjalizacja SparkSession
spark = SparkSession.builder \
    .appName("Gold Layer Aggregation") \
    .getOrCreate()

# Ścieżki do danych silver
input_path_internet = "/opt/data/silver/internet"
input_path_sms_call = "/opt/data/silver/sms_call"
output_path_gold = "/opt/data/gold/aggregated_data"

# Wczytanie danych silver
internet_data = spark.read.csv(input_path_internet, header=True, inferSchema=True)
sms_call_data = spark.read.csv(input_path_sms_call, header=True, inferSchema=True)

# Agregacja danych internetowych według countrycode
aggregated_internet = internet_data.groupBy("countrycode") \
    .agg(
        sum("transfer").alias("total_internet_transfer"),
        avg("transfer").alias("average_internet_transfer")
    )

# Agregacja danych SMS/Call według countrycode
aggregated_sms_call = sms_call_data.groupBy("countrycode") \
    .agg(
        sum("sms_count").alias("total_sms_count"),
        avg("sms_count").alias("average_sms_count"),
        sum("call_time").alias("total_call_time"),
        avg("call_time").alias("average_call_time")
    )

# Połączenie danych zagregowanych
aggregated_data = aggregated_internet.join(aggregated_sms_call, on="countrycode", how="outer")

# Zapisanie wyników do warstwy gold
aggregated_data.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_gold)

spark.stop()
