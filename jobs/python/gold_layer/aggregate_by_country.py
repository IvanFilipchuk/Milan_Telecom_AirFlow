from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import sum, avg
import sys

spark = SparkSession.builder \
    .appName("Gold Layer Aggregation by CountryCode") \
    .getOrCreate()

# internet_schema = StructType([
#     StructField("countrycode", IntegerType(), True),
#     StructField("transfer", DoubleType(), True)
# ])
#
# sms_call_schema = StructType([
#     StructField("countrycode", IntegerType(), True),
#     StructField("sms_count", DoubleType(), True),
#     StructField("call_time", DoubleType(), True)
# ])

input_path_internet = sys.argv[1] + "/internet"
input_path_sms_call = sys.argv[1] + "/sms_call"
output_path_gold_country = sys.argv[2]
partition_number = int(sys.argv[3])

# internet_data = spark.read.csv(input_path_internet, header=True, schema=internet_schema)
# sms_call_data = spark.read.csv(input_path_sms_call, header=True, schema=sms_call_schema)

internet_data = spark.read.csv(input_path_internet, header=True, inferSchema=True)
sms_call_data = spark.read.csv(input_path_sms_call, header=True, inferSchema=True)

aggregated_internet_country = internet_data.groupBy("countrycode") \
    .agg(
        sum("transfer").alias("total_internet_transfer"),
        avg("transfer").alias("average_internet_transfer")
    )
aggregated_internet_country.repartition(partition_number) \
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
aggregated_sms_call_country.repartition(partition_number) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_path_gold_country}/sms_call")

spark.stop()
