from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("ex").getOrCreate()

sc=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("id", IntegerType(), nullable=False),
    StructField("skills", StringType(), nullable=False)
])
df=spark.read.csv(r"C:\Users\Lakshmidevi\Downloads\array.csv",header=True,inferSchema=True)
df.show()
# df1=df.withColumn("array_new",array(col("name"),col("id")))
# df1.show()
# df3=df1.withColumn("array_con",array_contains("array_new","ram")).show()
#
# df3=df1.withColumn("array_size",size("array_new")).show()
#
# df3=df1.select("name",explode("array_new").alias("new"))
# df3.show()
#
# df3=df1.select("array_new",posexplode("array_new").alias("position", "Number"))
# df3.show()

#df3=df1.select("name",explode_outer("array_new").alias("new_out")).show()

d=df.withColumn("array_new",array(col("name"),col("skills")))
d.show()

d1=d.withColumn("array_co",array_contains("array_new","salesforce")).show()
d1=d.select("name",explode("array_new").alias("new_c"))
d1.show()

df.filter(df["name"].like("v%")).show()
d1=d.select("name",explode("array_new",)).alias("new_col").show()
d1=d.select("name",explode_outer("array_new").alias("new_c1")).show()

d1=d.select("name",posexplode("array_new").alias("position","number"))
d1.show()

d1=d.select("name",posexplode_outer("array_new").alias("position","number"))
d1.show()

df.agg(count("*")).show()
df.agg(sum(df["id"])).show()
df.agg(min(df["id"])).show()
df.agg(count(df["id"])).show()

df.withColumn("newup",upper("name")).show()
df.withColumn("ltrime_",trim("name")).show()

df.withColumn("name_l",length(col("name"))).show()

# df.groupBy("id").agg(count("id"),sum("id").show())

df.sort(df["name"].asc()).show()
#count particular column
# df.agg(count("*")).show()
# df.agg(count(df["name"])).show()
# df.agg(min(df["id"])).show()
# df.agg(min(df["id"])).show()
# df1=df.withColumn("new_c",lit("null")).show()

# df1.filter(df["id"].isNotNull()).show()
df.select("name").distinct().show()