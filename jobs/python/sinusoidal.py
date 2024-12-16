import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sin, rand, broadcast

spark = SparkSession.builder \
    .appName("Generate Synthetic Data") \
.config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

def generate_synthetic_data(input_file_path, num_records, output_file_path):
    data_df = spark.read.csv(input_file_path, header=True, inferSchema=True)
    sampled_df = data_df.sample(fraction=0.1, seed=42)
    grid_averages = sampled_df.groupBy("GridID").agg(
        avg("smsin").alias("avg_smsin"),
        avg("smsout").alias("avg_smsout"),
        avg("callin").alias("avg_callin"),
        avg("callout").alias("avg_callout"),
        avg("internet").alias("avg_internet")
    ).fillna(0)

    country_averages = sampled_df.groupBy("countrycode").agg(
        avg("smsin").alias("avg_smsin_country"),
        avg("smsout").alias("avg_smsout_country"),
        avg("callin").alias("avg_callin_country"),
        avg("callout").alias("avg_callout_country"),
        avg("internet").alias("avg_internet_country")
    ).fillna(0)
    grid_averages_broadcast = broadcast(grid_averages)
    country_averages_broadcast = broadcast(country_averages)
    joined_df = sampled_df.join(grid_averages_broadcast, on="GridID", how="left") \
                          .join(country_averages_broadcast, on="countrycode", how="left")
    synthetic_df = joined_df.withColumn(
        "smsin", (col("avg_smsin") + col("avg_smsin_country")) / 2 +
                  sin(rand() * 2 * 3.14159) * (col("avg_smsin") / 10)
    ).withColumn(
        "smsout", (col("avg_smsout") + col("avg_smsout_country")) / 2 +
                   sin(rand() * 2 * 3.14159) * (col("avg_smsout") / 10)
    ).withColumn(
        "callin", (col("avg_callin") + col("avg_callin_country")) / 2 +
                   sin(rand() * 2 * 3.14159) * (col("avg_callin") / 10)
    ).withColumn(
        "callout", (col("avg_callout") + col("avg_callout_country")) / 2 +
                    sin(rand() * 2 * 3.14159) * (col("avg_callout") / 10)
    ).withColumn(
        "internet", (col("avg_internet") + col("avg_internet_country")) / 2 +
                     sin(rand() * 2 * 3.14159) * (col("avg_internet") / 10)
    )
    synthetic_df = synthetic_df.select ("GridID", "TimeInterval","countrycode",
                                    "smsin", "smsout", "callin", "callout", "internet")
    synthetic_df = synthetic_df.limit(num_records)
    synthetic_df.repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_file_path)
input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
num_records = 1000

generate_synthetic_data(input_file_path, num_records, output_file_path)
