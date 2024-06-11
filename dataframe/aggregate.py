from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("example").getOrCreate()
options={'header':'true',
         'inferschema':'true',
         'delimeter':','}

path=r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\aggr.csv",
def create_read(format_type,options,path):
    return spark.read.format(format_type).options(**options).load(path)

read_df=create_read("csv",options,path)
read_df.show()

read_df.select(sum("salary")).show(truncate=False)
read_df.select(min("salary")).show()
read_df.select(mean("salary")).show()

read_df.select(collect_list("salary")).show(truncate=False)

df = read_df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df.show()
