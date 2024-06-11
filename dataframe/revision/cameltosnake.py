from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("ex").getOrCreate()
data = [("LakshmiDevi", 23), ("JanFeb", 45), ("FebMarch", 56)]
schema = ["NameFr", "AgeIn"]

df = spark.createDataFrame(data, schema)
df.show()

def camel_to_snake_case(s):
    res = ""
    for i in s:
        if i.isupper():
            res += '_' + i.lower()
        else:
            res += i
    return res.lstrip('_')

udf_camel_to_snake_case = udf(camel_to_snake_case, StringType())

# Apply camel_to_snake_case UDF to all columns
for col in df.columns:
    df = df.withColumnRenamed(col, udf_camel_to_snake_case(col))

df.show()
