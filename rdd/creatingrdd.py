from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("creating of data").getOrCreate()
data=[(3,4,5,8,6,20)]
sc= spark.sparkContext
files=sc.textFile(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\rdd\creating1.txt")
print(files.collect())






