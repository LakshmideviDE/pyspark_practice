from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Date Transformation") \
    .getOrCreate()

# Sample data
data = [("10-10/1996",),
        ("11/11-1997",),
        ("12/12/1998",)]

# Create DataFrame
df = spark.createDataFrame(data, ["col1"])
df.show()

# Replace "-" with "/"
df1= df.withColumn("col1",regexp_replace(df["col1"],'-','/'))
df1.show()

# # Convert to date type
# # df = df.withColumn("col2", to_date("col1", "dd/MM/yyyy"))
#
# # Extract date, month, and year
# df = df.withColumn("date", dayofmonth("col2")) \
#        .withColumn("month", month("col2")) \
#        .withColumn("year", year("col2"))
#
# # Show the DataFrame
# df.show()#

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark=SparkSession.builder.appName("example").getOrCreate()
# Define the custom schema
custom_schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", DateType(), nullable=True)
])
data=[("ram", 34,"plm"),
    ("jan",24,"blg"),("rup",56,"hyd")]
df=spark.createDataFrame(data=custom_schema, schema=data)

# Use the custom schema when reading data
#df = spark.read.csv("path/to/your/file.csv", schema=custom_schema)
df.show()
