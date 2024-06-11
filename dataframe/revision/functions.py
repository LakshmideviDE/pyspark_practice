# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName("ex").getOrCreate()
# options={'header':'true','inferSchema':'true','delimeter':','}
# path=r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sam1.csv"
# def read_csv(format_type,options,path):
#     return spark.read.format("csv").options(**options).load(path)
#
# r=read_csv("csv",options,path)
# r.show()


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("exa").getOrCreate()

cs=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("city",StringType(),nullable=False),
    StructField("gender",StringType(),nullable=False),
])
data=[("jan",23,'mpl','M'),('ram',34,'bng',"M"),('rupa',56,'plg','F'),('rupa',56,'plg','F')]
df1=spark.createDataFrame(data=data,schema=cs)
df1.show()

data1=[("rupesh",23,'blg','M'),('ram',34,'bng',"M"),('rupa',56,'plg','F')]
df2=spark.createDataFrame(data=data1,schema=cs)
df2.show()

d=df1.join(df2,df2.name==df1.name,'right').show()

#one column and adding
new=df1.withColumn("age_n", col("age")**2).show()

#drop duplicate particula column
d=df1.select("name").dropDuplicates((["name"])).show()

# #filter only males
# d = df.filter(df["gender"] == 'M')
# d.show()
# d=df.filter(df["gender"]=='F').show()
# #Update  m to male and f to female
#
# d=df.withColumn("gender",
#                 when(df["gender"]=='M',"Male")
#                 .when(df["gender"]=="F","Female")
#                 .otherwise(df["gender"]))

#
# date=df1.withColumn("date",date_add(current_date(),7))
# date.show()
#
# d=df1.withColumn("month",month(current_date())).show()
# d=df1.withColumn("monthfe",split(lit("2024-04-29"),'-')[1])
# d.show()
#
# d=df1.withColumn("new_d",date_diff(current_date(), df1(["date"])))
# d.show()


d1=df1.withColumn("gender",
                 when(df1["gender"]=="M","male")
                 .when(df1["gender"]=='F','Female'))

d1.show()

df=df1.filter(df1["name"]=='ram')
df.show()


