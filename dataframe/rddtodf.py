
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("creating of data").getOrCreate()

df = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\rdd\first.csv", header=True, inferSchema=True)
#df.select("name").show()
df.printSchema()
#