import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Silver Layer - Internet Data") \
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

internet_data = data.filter(col("internet").isNotNull() & (col("internet") != "0")) \
    .withColumn("transfer", col("internet").cast(DoubleType())) \
    .select("GridID", "TimeInterval", "countrycode", "transfer")

internet_data.repartition(partition_number) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()
