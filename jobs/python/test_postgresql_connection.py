from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgreSQL Connection Test") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://sparkingflow-postgres-1:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

data = [(1, "Test1"), (2, "Test2"), (3, "Test3")]
columns = ["id", "name"]

df = spark.createDataFrame(data, columns)

df.write.jdbc(url=jdbc_url, table="test_table", mode="overwrite", properties=properties)

read_df = spark.read.jdbc(url=jdbc_url, table="test_table", properties=properties)
read_df.show()
