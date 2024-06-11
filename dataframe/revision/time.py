from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("exa").getOrCreate()

schema_df = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("date_str", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False)
])

data = [("lakshmi", "2000-12-26", 23), ("chandu", "2001-08-09", 24)]
df = spark.createDataFrame(data=data, schema=schema_df)
df.show()
df.printSchema()

# df = df.withColumn("days_diff", datediff(current_date(), df["date_str"]))
# df.show()

df =df.select(date_format(current_date(), "yyyy-MM-dd").alias("formatted_date"))
df.show()


# df = df.withColumn("date_str", to_date(col("date_str").cast(DateType())))
# df.show()
# df.printSchema()
#
# df = df.withColumn("date_str", datediff(df["2000-12-26"], df["2001-08-09"]))
# df.show()
#
#
#
#
#
#
#
#
#





# df=spark.read.csv(r"C:\Users\Lakshmidevi\Downloads\task2.csv", inferSchema=True, schema=schema_df)
# df.show()
# df.withColumn("new_time",lit(current_timestamp()).show()
# df1=df.withColumn("date_adding", date_add(current_date(), 8))
# df1.show()
#
# df_w = df.withColumn("new", date_add(current_date(), 8))
# df_w.show()
# df_w=df.withColumn("month1",month(current_date()))
# df_w.show()
# df_w=df.withColumn("yearfetch",year(current_date()))
# df_w.show()
#
df_w=df.withColumn("dayfetch",split(lit("2888-2-23"),'-')[2]).show()
# #
# df_w=df.withColumn("yerfet",split(lit("1999-12-23"),'-')[0]).show()









# ns=StructType([
#     StructField('f_name',StringType(),nullable=False),
#     StructField("l_name",StringType(),nullable=False)
# ])
# sc=StructType([
#     StructField("id",IntegerType(),nullable=False),
#     StructField("name",ArrayType(ns),nullable=False),
#     StructField("city",StringType(),nullable=False)])
# data=[(1,[('a','b')],'mpl'),(2,[('c','d')],'mpl')]
# df=spark.createDataFrame(data=data,schema=sc)
# df.show()
# df.dropDuplicates(["city"]).show()
# df.select("city").dropDuplicates(["city"]).show()