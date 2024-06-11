from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Pivot Example").getOrCreate()

data = [("ram", "Apple", 10),
        ("ram", "Orange", 5),
        ("Jan", "Apple", 8),
        ("Jan", "Orange", 7)]

df = spark.createDataFrame(data, ["Name", "Fruit", "Quantity"])
pivot_df = df.groupBy("Name").pivot("Fruit").sum("Quantity")

pivot_df.show()

