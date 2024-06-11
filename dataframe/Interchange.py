from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("example").getOrCreate()

df = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sam1.csv", header=True,
                    inferSchema=True)
df.show()

changed_df = df.withColumn("temp", col("firstname")) \
                    .withColumn("firstname", col(" lastname")) \
                    .withColumn(" lastname", col("temp")) \
                    .drop("temp")

changed_df.show()

