# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
#
# spark = SparkSession.builder.appName("example").getOrCreate()
#
# custom1 = StructType([
#     StructField("name", StringType(), nullable=False),
#     StructField("salary", IntegerType(), nullable=False),
#     StructField("gender", StringType(), nullable=False)
# ])
#
# path = r"C:\Users\Lakshmidevi\Downloads\jsontask3.json"
#
# df = spark.read.format("json").option('multiline', True).schema(custom1).load(path)
# df.show()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ex").getOrCreate()
schema_df = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("salary", StringType(), nullable=False),
    StructField("gender", StringType(), nullable=False)
])
path = r"C:\Users\Lakshmidevi\Downloads\jsontask3.json"
df = spark.read.format("json").option('multiline', True).schema(schema_df).load(path)
df.show()

# handle null valuesw(
# name_count = df.select("name").where(df["name"].count()).show()


up = df.withColumn("salary_df",
                   when(df["salary"] <= 900, "low").when(df["salary"] >= 600, "high").otherwise(df["salary"]))
up.show()

df = df.withColumn("new_c", lit("Null"))
up.printSchema()
# up=df.filter(df.new_c.isNotNull())
# up.show()
# df=df.fillna("key", subset=["new_c"]).show()
# res={"new_c":"hii"}
# df.fillna(res).show()


df=df.withColumn("gender",
                 when(df["gender"]=='M','male')
                 .when(df["gender"]=="F","female")
                 .otherwise(df["gender"]))
df.show()
