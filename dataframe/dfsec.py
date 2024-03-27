from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Simple dataframe").getOrCreate()
data=[("lakshmi", 22),("chandu",20),("rani",34)]
df = spark.createDataFrame(data,["name","age"])
#df.collect()
df_c= df.columns
print(df_c)

