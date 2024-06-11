from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("array example").getOrCreate()

options = {'header': 'true', 'inferSchema': 'true', 'delimiter': ','}
path = r"C:\Users\Lakshmidevi\Downloads\array.csv"
read = spark.read.format("csv").options(**options).load(path)
read.show()
df1= read.withColumn('array_df',array(col('name'),col('id')))
df1.show()
array_df= df1.withColumn("contains_value", array_contains("array_df", "jan"))
array_df.show()
array_df =df1.withColumn("array_size", size("array_df"))
array_df.show()
#
array_df=df1.withColumn("array_pos", array_position("array_df","ram"))
array_df.show()
#
# array_df=read.withColumn("array_remove",array_remove("array_df","jan"))
# array_df.show()
array_df=df1.select("name",explode("array_df").alias("newexplored"))
array_df.show()
array_df= df1.select("name", explode_outer("array_df").alias("array_out"))
array_df.show()

array_df =df1.select("name", posexplode_outer("array_df").alias("newcol", "number"))
array_df.show()

array_df =df1.select("name", posexplode("array_df").alias("Position", "Number"))
array_df.show()

# female_df = df.filter(df['gender'] == 'female')
# female_df.show()
