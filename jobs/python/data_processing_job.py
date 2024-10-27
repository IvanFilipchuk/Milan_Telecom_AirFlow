from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataProcessingJob").getOrCreate()

data_path = "/opt/airflow/data1.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

df = df.limit(10000)

if 'smsin' in df.columns:
    words = df.select("smsin").rdd.flatMap(lambda row: str(row[0]).split(" "))
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    for wc in word_counts.collect():
        print(wc[0], wc[1])
else:
    print("Kolumna 'smsin' nie istnieje w danych")

spark.stop()
