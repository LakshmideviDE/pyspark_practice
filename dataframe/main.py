from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a SparkSession
spark = (SparkSession.builder\
    .appName("Simple Example of pyspark")\
         .getOrCreate())
data = [(1, "lakshmi"), (2, "chandu")]

# schema = ["id", "name"]
schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=StringType())])
# Create DataFrame

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df1 = df.count()








