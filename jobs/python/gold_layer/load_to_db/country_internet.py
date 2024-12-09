from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Load Country Internet Data to PostgreSQL") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

csv_path = sys.argv[1]
table_name = sys.argv[2]

jdbc_url = "jdbc:postgresql://sparkingflow-postgres-1:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

spark.stop()
