from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

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

input_path = sys.argv[1]
output_path = sys.argv[2]
partition_number = int(sys.argv[3])

data = spark.read.csv(input_path, header=True, schema=schema)

sample_data = data.limit(10000)

# sample_data.write \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .partitionBy("countrycode") \
#     .csv(output_path)

sample_data.repartition(partition_number) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()
