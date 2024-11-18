import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("Silver Layer") \
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
output_path_internet = sys.argv[2]+"/internet"
output_path_sms_call = sys.argv[2]+"/sms_call"
output_path_user_events = sys.argv[2]+"/user_events"

data = spark.read.csv(input_path, header=True, schema=schema)

internet_data = data.filter(col("internet").isNotNull() & (col("internet") != "0")) \
    .withColumn("transfer", col("internet").cast(DoubleType())) \
    .select("GridID", "TimeInterval", "countrycode", "transfer")

internet_data.repartition(2) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_internet)

sms_call_data = data.filter((col("smsin").isNotNull() & (col("smsin") != "0")) |
                            (col("smsout").isNotNull() & (col("smsout") != "0")) |
                            (col("callin").isNotNull() & (col("callin") != "0")) |
                            (col("callout").isNotNull() & (col("callout") != "0"))) \
    .withColumn("sms_count", (col("smsin").cast(DoubleType()) + col("smsout").cast(DoubleType()))) \
    .withColumn("call_time", (col("callin").cast(DoubleType()) + col("callout").cast(DoubleType()))) \
    .select("GridID", "TimeInterval", "countrycode", "sms_count", "call_time")

sms_call_data.repartition(2) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_sms_call)

user_events = data.withColumn("traffic_type", expr("CASE WHEN smsin IS NOT NULL THEN 'smsin' " +
                                                   "WHEN smsout IS NOT NULL THEN 'smsout' " +
                                                   "WHEN callin IS NOT NULL THEN 'callin' " +
                                                   "WHEN callout IS NOT NULL THEN 'callout' " +
                                                   "WHEN internet IS NOT NULL THEN 'internet' " +
                                                   "ELSE 'unknown' END")) \
    .withColumn("traffic_amount", expr("CASE WHEN traffic_type IN ('smsin', 'smsout') THEN 5 " +
                                       "WHEN traffic_type IN ('callin', 'callout') THEN 3.5 " +
                                       "WHEN traffic_type = 'internet' THEN 10 " +
                                       "ELSE 0 END")) \
    .selectExpr("GridID as grid_id_visited", "countrycode as customer_Identifier", "traffic_type", "traffic_amount")

user_events.repartition(2) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_user_events)

spark.stop()