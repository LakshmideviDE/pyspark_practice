from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark=SparkSession.builder.appName("ex").getOrCreate()

sc=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("address",StringType(),nullable=False),
])
data=[("lakshmi",23,"plm"),("ram",65,"bng")]

df=spark.createDataFrame(data=data,schema=sc)
window_s=Window.orderBy("age")
n=df.withColumn("rank",rank().over(window_s))
n.show()
m=df.withColumn("lead",lead("age").over(window_s)).show()
m=df.withColumn("lag",lag("age").over(window_s)).show()
m=df.withColumn("dense_c",dense_rank().over(window_s)).show()