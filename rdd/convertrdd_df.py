from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("example").getOrCreate()

schema=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("gender",StringType(),nullable=False)
])
data=[("lakshmi",23,"mpl","f"),
("ram",34,"plm","m")
]
df=spark.createDataFrame(data,schema)
df.show()
#notnull values return esthedi first coalesce
# df1=df.withColumn("new_c", coalesce("name"))
# df1.show()
# df1=df.fillna("name")
# df1=df.na.drop(subset=["name"])
# df1.show()
# df_filled = df.na.fill(value={"age": 0, "name": "unknown"})  # Replace null values in specific columns with specified values
# df_filled.show()
# df1=df.select("name")
# df1.show()
# df8=df.withColumn("gender",lit("m"))
# df8.show()
# # df_updated = df.withColumn("gender",
# # grouped_df = df.groupBy("city").agg(count("*").alias("count"))
# #
# # # Show the result
# # grouped_df.show()
df.withColumn("city",when(df.city=="mpl","btm").otherwise(df.city)).show()

df.withColumn("city",when(df.city=='btm','mpl').otherwise(df.city)).show()

df1=df.filter(df["gender"] == "m")
df1.show()