# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName("example").getOrCreate()
#
# # Sample data in RDD
# data = [("Alice", 34), ("Bob", 45), ("Charlie", 56)]
#
# # Create an RDD
# rdd = spark.sparkContext.parallelize(data)
#
# # Convert RDD to DataFrame
# df = rdd.toDF(["Name", "Age"])
#
# # Show DataFrame
# df.show()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("example").getOrCreate()
#
# data=[("lakshmi",56),("dfgg",78),("sdfg",67)]
#
# schema=["name","age"]
#
# df=spark.createDataFrame(data=data,schema=schema)
# df.show()


# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import *
#
# spark = SparkSession.builder.appName("example").getOrCreate()
#
# schema=StructType([
#     StructField("name",StringType(),nullable=False),
#     StructField("age",IntegerType(),nullable=False)
# ])
# data=[("lakshmi",24),("asd",56)]
# df=spark.createDataFrame(data,schema)
# df.show()
# res=df.filter(df["age"]==56)
# res.show()

# res=df.withColumn("new_",lit("260"))
# res.show()

# target_name = "lakshmi"
# target_age = 36  # New age value
#
# # Apply the update
# updated_df = df.withColumn("age",
#     when(df["name"] == target_name, target_age).otherwise(df["age"])
# )
# updated_df.show()


# update=df.withColumn("age",
#                      when(df["name"]== "lakshmi",34).otherwise(df["age"]))
# update.show()
# udpate=df.withColumn("age",when(["name"]=="lakshmi",23).otherwise(df["age"]))

#create rdd
# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName("example").getOrCreate()
#
#
# data=[("svdbn",34),("sdghj",54)]
#
# rdd=spark.sparkContext.parallelize(data)
# print(rdd.collect())
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("example").getOrCreate()

schema=StructType([
    StructField("name", StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False)
])

data=[("ram",34),("asd",23),("asds",24)]
df=spark.createDataFrame(data,schema)
df.show()

#add column
# res=df.withColumn("new",lit(34))
# res.show()
# #update column
#
# res=df.withColumn("age",df["age"]+1)
# res.show()
#
# #update one value
# up=df.withColumn("age",when(df["name"]=="ram",56).otherwise(df["age"]))
# up.show()
#display unique values
# dp=df.select("name").distinct()
# dp.show()
# #filter only age 34
# dp=df.filter(df["age"]==34)
# dp.show()
#
# dp=df.select("name").show()
#
# dp=df.withColumn("gender",lit('F'))
# dp.show()
#
# #update one value
# dp=df.withColumn("age",
#                  when(df["name"]=="ram",45).otherwise(df["age"]))
#
#
#
# dp=df.withColumn("name",
#                  when(df["age"]==23,"lakshmi").otherwise(df["name"]))
# dp.show()


#display r letter names
# df=df.filter(col("name").like("r%")).select("name")
# df.show()

#update age 34
# dp=df.select(col("name"))
# dp.show()

# dp = df.withColumn("name", col("name").cast("Integer"))
# dp.show()
# dp.printSchema()
dp=df.withColumn("age",col("age").cast("Integer"))
dp.printSchema()
#column change
dp=df.withColumnRenamed("name","full_name")
dp.show()

dp=df.withColumn("age",
                 when(df["age"]==34,60).when(df["age"]==23,26).otherwise(df["age"]))
dp.show()

#update all values in age
updated=df.withColumn("age",(df["age"]+1))
updated.show()
up=df.withColumn("age",col("age")+1)
up.show()
#concatenate
dp=df.withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name")))
dp.show()

dp=df.withColumn("age",col("age")+1)
