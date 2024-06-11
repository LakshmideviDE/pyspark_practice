from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("creating of data").getOrCreate()

df1 = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\u1.csv",header=True,inferSchema=True)
df2 = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\u2.csv",header=True,inferSchema=True)
# df=df1.printSchema()
# df=df2.printSchema()
df=df1.union(df2)
df.show()
df=df1.union(df2).drop_duplicates()
df.show()
df=df1.union(df2).distinct()
df.show()

df=df1.unionAll(df2)
df.show()










# df=df1.unionAll(df2)
# df.show()
# df=df1.union(df2).drop_duplicates()
# df.show()
# # union_df=df1.unionByName(df2,allowMissingColumns=True)
# # union_df.show()
