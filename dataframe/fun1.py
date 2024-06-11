from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("example").getOrCreate()

options={'header':'true',
         'inferSchema':'true',
         'delimiter':','}
path=r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sam1.csv"

def read_file(format_type, options, path):
    return spark.read.format(format_type).options(**options).load(path)

read_df=read_file("csv", options, path)
read_df.show()

def create_age_column(df,column_name, value):
   return df.withColumn(column_name, lit(value))
#
read_df=create_age_column(read_df, "Age", 20)
#
#
def drop_column(df,column_name):
    return df.drop(column_name)
#
drop_column_df=drop_column(read_df,"Age")
drop_column_df.show()
#
# def add_gender(df,column_name,value):
#     return df.withColumn(column_name, lit(value))
# red_df=add_gender(read_df,'gender', 'm')
# red_df.show()

# def drop_age(df,column_name,value):
#     return df.drop(column_name)
# drop=drop_age(read_df,'age',20)
