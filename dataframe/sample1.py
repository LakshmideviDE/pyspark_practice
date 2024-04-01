from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Parquet Read Example") \
    .getOrCreate()
# df = spark.read.json(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\file1.json")
# df.show()
df= spark.read.parquet(r"C:\Users\Lakshmidevi\Downloads\MT cars.parquet")
df.show()