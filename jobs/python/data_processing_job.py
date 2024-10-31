from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("DataProcessingJob") \
    .config("spark.master", "local") \
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

input_path = "/opt/airflow/data1.csv"
output_path = "/opt/airflow/bronze/sample_data"

data = spark.read.csv(input_path, header=True, schema=schema)

sample_data = data.limit(10000)

sample_data.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("countrycode") \
    .csv(output_path)

spark.stop()
