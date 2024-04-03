from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("creating of data").getOrCreate()

df1 = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\union.csv",header=True,inferSchema=True)
df=df1.printSchema()
window_s=Window.orderBy("salary").partitionBy("dep")
new=df1.withColumn("row_num",row_number().over(window_s))
new.show()

new=df1.withColumn("rank",rank().over(window_s))
new.show()

new=df1.withColumn("dense_rank",dense_rank().over(window_s))
new.show()

d=df1.withColumn("lead",lead("salary").over(window_s))
d.show()

d=df1.withColumn("lag",lag("salary").over(window_s))
d.show()