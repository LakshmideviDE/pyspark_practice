import columns
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Simple dataframe").getOrCreate()
data=[("lakshmi", 22),("chandu",20),("rani",34)]
df = spark.createDataFrame(data,["name","age"])
df.show()
#df.show(truncate=False)
#df.show(2,truncate=False)
#df.show(2,truncate=20)
#df.show(1,truncate=20, vertical=True)
#df.show(1,truncate=False, vertical=True)
#df.select("name", "age").show()
#df.select(df.name, df.age).show()
df.select(df["name"],df["age"]).show()
df.select(col("name"),col("age")).show()

#df.select(*columns).show()
#df.select([col for col in df.columns]).show()
#df.select("*").show()
#df.select(df.columns[:2]).show(2)


#df2=df.show(truncate=False) # shows all columns
#print(df2)
unique_col=df.distinct().count()
print(unique_col)


from pyspark.sql.functions import count
df.select(count(df.name)).show()
df.select(count(df.name), count(df.age)).show()
df.groupBy("name").count().show()

column_names = df.columns
column_names.show()