from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("simple example").getOrCreate()
data1=[("lakshmi", 23, "bangalore"),
       ("jan", 34, "mpl"),
       ("ram", 54, "delhi")]

schema = ["name", "age", "city"]

df=spark.createDataFrame(data1,schema)

df.show()

# df_r = df.select("age")
# df_r.show()
# df_r =df.withColumnRenamed("city","location")
# df_r.show()
#
# df_r=df.withColumn("age_5", col("age") + 5)
# df_r.show()
# df_x=df.withColumn("new_n",col("name"))
# df_x.show()
#
# df_x=df.drop("new_n")
# df_x.show()
#
# s=df.select("age")
# s.show()

# df_f=df.filter(col("age")>25)
# df_f.show()
# options={"header":"true", "inferSchema":"true", "delimeter":','}
# path= r"C:\Users\Lakshmidevi\Downloads\array.csv"
# read= spark.read.format("csv").options(**options).load(path)
# read.show()
# df=spark.read.csv(r"C:\Users\Lakshmidevi\Downloads\array.csv", header=True, inferSchema=True)
# df.show()

# df=spark.read.json(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\rdd\pra.json")
# df.show()

# female_df = df.filter(df['gender'] == 'female')
# female_df.show()

female_df =df.withColumn("gender",lit('f'))
female_df.show()

#res=df.filter(df['gender']== 'female')
res=df.filter(df["age"]>25)
res.show()
res=df.filter(col("age")<23)
res.show()
from pyspark.sql import SparkSession

# Create a SparkSession
# spark = SparkSession.builder \
#     .appName("RDD Example") \
#     .getOrCreate()
#
# # Create an RDD from a list of elements
# data = [1, 2, 3, 4, 5]
# rdd = spark.sparkContext.parallelize(data)
#
# # Show RDD elements
# print("RDD Elements:", rdd.collect())

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [("ram", 23), ("jan", 45), ("rup", 56)]

# Create an RDD
rdd = spark.sparkContext.parallelize(data)
df=rdd.toDF(["name","age"])
df.show()

# Collect and print the RDD
#print(rdd.collect())
from pyspark.sql import SparkSession

spark =SparkSession.builder.appName("ex").getOrCreate()

data=[("ram",56),("sds",23)]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

