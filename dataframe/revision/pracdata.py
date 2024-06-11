from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("ex").getOrCreate()
data = [(1,"India",2000), (2,"us",3000), (3,"FebMarch",5000)]
schema = ["id", "country","salary"]

df = spark.createDataFrame(data, schema)
df.show()
df1=df.orderBy(asc(df["country"]))
df1.show()

df1=df.groupBy("id","country").count()
df1.show()
df1=df.groupBy("id").agg(avg("salary")).show()

df=df.withColumn("at_column_date",lit(0)).show()
#List the department names along with their corresponding country names.
df.select("id","name").show()