from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Example").getOrCreate()
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("location", StringType(), True)
])
data = [("vani", "23", "bng"), ("jan", "24", "plm")]

# Create a DataFrame from the provided data and schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
#df1=df.select("*")
#print(df1)
# new_df=df.withColumn("location", df["age"] *2)
# print(new_df)
# df1=df.filter("age>18")
# print(df1)
# df.filter(df.location=="bng").show()
#
# rows=df.count()
# print(rows)
#
# df1 = df.withColumn("substring_column", substring(col("name"), 2, 4))
# df1.show()
# df1 = df.withColumn("Upper_case_column", upper(col("name")))
# df1.show()
#
# trimmed_df = df.withColumn("trimmed_column", ltrim(col("name")))
# trimmed_df.show()

# split_df = df.withColumn("split_column", split(col("name"), ","))
# split_df.show()
#
# repeated_df = df.withColumn("repeated_column", repeat(col("name"), 3))
# repeated_df.show()

padded_df = df.withColumn("padded_column", rpad(col("name"), 10, "0"))
padded_df.show()