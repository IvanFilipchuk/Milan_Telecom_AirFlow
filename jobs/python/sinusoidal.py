from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sin, cos, monotonically_increasing_id, coalesce, row_number
from pyspark.sql.window import Window
import math
import sys

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Generate Synthetic Data for 1000 Records") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Database connection parameters
jdbc_url = "jdbc:postgresql://milan_telecom_airflow-postgres-1:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Input arguments
table_country_sms_calls = sys.argv[1]  # Aggregation by country code (SMS/calls)
table_country_internet = sys.argv[2]  # Aggregation by country code (Internet)
table_grid_sms_calls = sys.argv[3]    # Aggregation by GridID (SMS/calls)
table_grid_internet = sys.argv[4]     # Aggregation by GridID (Internet)
output_path = sys.argv[5]             # Path to save the final output CSV

# Sinusoidal function parameters
time_period = 24  # Time period for sinusoidal wave (e.g., 24 hours)
time_interval_ms = 600000  # Time interval in milliseconds (e.g., 10 minutes)
start_time_ms = 1383260400000  # Starting timestamp in milliseconds

# Generate time points
time_points = [start_time_ms + i * time_interval_ms for i in range(time_period)]
time_df = spark.createDataFrame(time_points, "long").toDF("TimeInterval")

# Helper function to read and preprocess tables
def read_and_prepare_table(table_name, key_column, total_columns):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
    for col_name in total_columns:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0)))
    return df

# Read tables
df_country_sms_calls = read_and_prepare_table(table_country_sms_calls, "countrycode", ["total_sms_count", "total_call_time"])
df_country_internet = read_and_prepare_table(table_country_internet, "countrycode", ["total_internet_transfer"])
df_grid_sms_calls = read_and_prepare_table(table_grid_sms_calls, "GridID", ["total_sms_count", "total_call_time"])
df_grid_internet = read_and_prepare_table(table_grid_internet, "GridID", ["total_internet_transfer"])

# Combine country-level data
country_data = df_country_sms_calls.join(df_country_internet, "countrycode", "outer")

# Combine grid-level data
grid_data = df_grid_sms_calls.join(df_grid_internet, "GridID", "outer")

# Add proportions for sampling
def calculate_proportions(df, total_column):
    total_value = df.selectExpr(f"sum({total_column}) as total").collect()[0]["total"]
    return df.withColumn("proportion", col(total_column) / total_value)

country_data = calculate_proportions(country_data, "total_sms_count")
grid_data = calculate_proportions(grid_data, "total_sms_count")

# Combine all data and assign GridIDs to country-level data
all_data = country_data.unionByName(grid_data, allowMissingColumns=True)

# Add time intervals
all_data = all_data.crossJoin(time_df)

# Generate sinusoidal data
all_data = all_data.withColumn(
    "smsin", sin(2 * math.pi * col("TimeInterval") / time_period) * coalesce(col("total_sms_count"), lit(0))
).withColumn(
    "smsout", cos(2 * math.pi * col("TimeInterval") / time_period) * coalesce(col("total_sms_count"), lit(0))
).withColumn(
    "callin", sin(2 * math.pi * col("TimeInterval") / time_period + math.pi / 4) * coalesce(col("total_call_time"), lit(0))
).withColumn(
    "callout", cos(2 * math.pi * col("TimeInterval") / time_period + math.pi / 4) * coalesce(col("total_call_time"), lit(0))
).withColumn(
    "internet", sin(2 * math.pi * col("TimeInterval") / time_period + math.pi / 2) * coalesce(col("total_internet_transfer"), lit(0))
)

# Add unique ID and limit to 1000 records
window_spec = Window.orderBy(monotonically_increasing_id())
final_data = all_data.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") <= 1000)

# Select required columns
final_data = final_data.select(
    col("GridID"),
    col("countrycode"),
    col("TimeInterval"),
    col("smsin"),
    col("smsout"),
    col("callin"),
    col("callout"),
    col("internet")
)

# Save the final CSV
final_data.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
