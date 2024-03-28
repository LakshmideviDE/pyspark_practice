from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
#df.show()
#df1=df.select("*")
#print(df1)
# new_df=df.withColumn("location", df["age"] *2)
# print(new_df)
# df1=df.filter("age>18")
# print(df1)
df.filter(df.location=="bng").show()

rows=df.count()
print(rows)
