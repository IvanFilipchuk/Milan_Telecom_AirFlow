import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Silver Layer - SMS and Call Data") \
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

input_path = sys.argv[1]
output_path = sys.argv[2]
partition_number = int(sys.argv[3])

data = spark.read.csv(input_path, header=True, schema=schema)

sms_call_data = data.filter((col("smsin").isNotNull() & (col("smsin") != "0")) |
                            (col("smsout").isNotNull() & (col("smsout") != "0")) |
                            (col("callin").isNotNull() & (col("callin") != "0")) |
                            (col("callout").isNotNull() & (col("callout") != "0"))) \
    .withColumn("sms_count", (col("smsin").cast(DoubleType()) + col("smsout").cast(DoubleType()))) \
    .withColumn("call_time", (col("callin").cast(DoubleType()) + col("callout").cast(DoubleType()))) \
    .select("GridID", "TimeInterval", "countrycode", "sms_count", "call_time")

sms_call_data.repartition(partition_number) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()
