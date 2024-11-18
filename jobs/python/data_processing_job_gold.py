from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg
spark = SparkSession.builder \
    .appName("Gold Layer Aggregation") \
    .getOrCreate()
input_path_internet = "/opt/data/silver/internet"
input_path_sms_call = "/opt/data/silver/sms_call"
output_path_gold_grid = "/opt/data/gold/aggregated_by_gridid"
output_path_gold_country = "/opt/data/gold/aggregated_by_country"
internet_data = spark.read.csv(input_path_internet, header=True, inferSchema=True)
sms_call_data = spark.read.csv(input_path_sms_call, header=True, inferSchema=True)

# --- Agregacja po GridID ---
aggregated_internet_grid = internet_data.groupBy("GridID") \
    .agg(
        sum("transfer").alias("total_internet_transfer"),
        avg("transfer").alias("average_internet_transfer")
    )
aggregated_internet_grid.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_path_gold_grid}/internet")
aggregated_sms_call_grid = sms_call_data.groupBy("GridID") \
    .agg(
        sum("sms_count").alias("total_sms_count"),
        avg("sms_count").alias("average_sms_count"),
        sum("call_time").alias("total_call_time"),
        avg("call_time").alias("average_call_time")
    )
aggregated_sms_call_grid.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_path_gold_grid}/sms_call")

# --- Agregacja po countrycode ---

aggregated_internet_country = internet_data.groupBy("countrycode") \
    .agg(
        sum("transfer").alias("total_internet_transfer"),
        avg("transfer").alias("average_internet_transfer")
    )
aggregated_internet_country.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_path_gold_country}/internet")
aggregated_sms_call_country = sms_call_data.groupBy("countrycode") \
    .agg(
        sum("sms_count").alias("total_sms_count"),
        avg("sms_count").alias("average_sms_count"),
        sum("call_time").alias("total_call_time"),
        avg("call_time").alias("average_call_time")
    )
aggregated_sms_call_country.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_path_gold_country}/sms_call")

spark.stop()
