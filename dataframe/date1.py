from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("date example").getOrCreate()


options={'header':'true',
         'inferSchema':'true',
         'delimiter':','}
path=r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sam1.csv"

def read_file(formate_type,options,path):
    return spark.read.format(formate_type).options(**options).load(path)
read= read_file("csv", options, path)
read.show()

def create_current(df,column_name, value):
    return df.withColumn(column_name, lit(value))
read_df = create_current(read, "current_date", current_date())
read_df.show()

def create_time(df,column_name,value):
    return df.withColumn(column_name, lit(value))
read_df=create_time(read,"new_time", current_timestamp())
read_df.show()

