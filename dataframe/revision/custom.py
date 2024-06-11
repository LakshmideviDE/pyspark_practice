from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("exa").getOrCreate()

schema_df=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("address",StringType(),)
])
df=spark.read.csv(r"C:\Users\Lakshmidevi\Downloads\task2.csv", inferSchema=True, schema=schema_df)
df.show()


# #one column
# df=df.withColumn("gender",lit("F"))
# df.show()
#
# #Renamed colum
# df=df.withColumnRenamed("name","Full_name")
# df.show()
# #chnage the data type
# df=df.withColumn("age",col("age").cast("String"))
# df.printSchema()
# #when and other column
#
# df=df.withColumn("gender",
#                  when(df["full_name"]=="ram","M").otherwise(df["gender"]))
# df.show()
#
# #null values handle
# df=df.withColumn("newcol",lit("null")).show()
#
# #df=df.fillna("hii", subset=["newcol"]).show()
# # df_filled = df.fillna("hii", subset=["newcol"])
# # Assuming `df` is your DataFrame and you want to replace null values in the "newcol" column with the value "hw"
# # res = {"newcol": "hw"}
# df_filled = df.fillna(res)
# df_filled.show()
#remove null values
# df= df.na.drop().show()



# # Function to convert camel case to snake case
# def camel_to_snake_case(df):

#udf(camel_to_snake_case)

# def camel_to_snake_case(name):
#     # Convert camel case to snake case
#     snake_case = ''.join(['_' + c.lower() if c.isupper() else c for c in name]).lstrip('_')
#     return snake_case
# print(camel_to_snake_case("chandu")
