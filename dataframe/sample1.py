from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Parquet Read Example") \
    .getOrCreate()
df= spark.read.parquet(r"C:\Users\Lakshmidevi\Downloads\MT cars.parquet")
df.show()