from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import shutil
import os

spark = SparkSession.builder \
    .appName("Bronze Layer") \
    .getOrCreate()

schema = StructType([
    StructField("GridID", IntegerType(), True),
    StructField("TimeInterval", StringType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", StringType(), True),
    StructField("smsout", StringType(), True),
    StructField("callin", StringType(), True),
    StructField("callout", StringType(), True),
    StructField("internet", StringType(), True)
])

input_path = "/opt/data/data1.csv"
output_path = "/opt/data/bronze/sample_data"

if os.path.exists(output_path):
    try:
        shutil.rmtree(output_path)
    except PermissionError as e:
        print(f"Permission error: {e}")

data = spark.read.csv(input_path, header=True, schema=schema)

sample_data = data.limit(10000)

# sample_data.write \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .partitionBy("countrycode") \
#     .csv(output_path)

sample_data.repartition(2) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()
