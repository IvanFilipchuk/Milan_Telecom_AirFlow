import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("Silver Layer - User Events") \
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

user_events = data.withColumn("traffic_type", expr(
    "CASE WHEN smsin IS NOT NULL THEN 'smsin' "
    "WHEN smsout IS NOT NULL THEN 'smsout' "
    "WHEN callin IS NOT NULL THEN 'callin' "
    "WHEN callout IS NOT NULL THEN 'callout' "
    "WHEN internet IS NOT NULL THEN 'internet' "
    "ELSE 'unknown' END")) \
    .withColumn("traffic_amount", expr(
        "CASE WHEN traffic_type IN ('smsin', 'smsout') THEN 5 "
        "WHEN traffic_type IN ('callin', 'callout') THEN 3.5 "
        "WHEN traffic_type = 'internet' THEN 10 "
        "ELSE 0 END")) \
    .selectExpr("GridID as grid_id_visited", "countrycode as customer_Identifier",
                "traffic_type", "traffic_amount")

user_events.repartition(partition_number) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()
