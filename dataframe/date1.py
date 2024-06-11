from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("date example").getOrCreate()

options = {'header': 'true',
           'inferSchema': 'true',
           'delimiter': ','}
path = r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sam1.csv"


def read_file(formate_type, options, path):
    return spark.read.format(formate_type).options(**options).load(path)


read = read_file("csv", options, path)
read.show()


def create_current(df, column_name, value):
    return df.withColumn(column_name, lit(value))


read_df = create_current(read, "current_date", current_date())
read_df.show()
df_with = read_df.withColumn("NewColumn", current_timestamp())
df_with.show()
df_w = read.withColumn("new", date_add(current_date(), 8))
df_w.show()

d_l = read.withColumn("month", month(current_date()))
d_l.show()

# d_l=read.withColumn("month",split(lit("2022-06-26"),'-')[0])
# d_l.show()

d_l = read.withColumn("month", split(lit("2022-02-6"), '-')[1])
d_l.show()

# df_l = read.withColumn("date_column", to_date(read["date_column"], "2024-03-3"))

read = read.withColumn("date_column", current_date())
read.show()
df_l = read.withColumn("date_column", to_date(read["date_column"], "yyyy-MM-dd"))
df_l.show()
read.printSchema()
#date to string formate converted
# df_l=read.withColumn("date_column",date_format(read["date_column"],"yyyy-mm-dd"))
# df_l.printSchema()

df_l=read.withColumn("year",date_add(current_date()))
df_l.show()

diff_days_df = df_l.withColumn("date_diff", datediff(col("23-03-2024"), col("2024-04-08")))
diff_days_df.show()

df.withColumn("new_col",lit("Lakshmi_devi"))
df.withColumn("new_cols",split("new_col","_"))