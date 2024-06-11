from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("ex").getOrCreate()

sc=StructType([
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("address",StringType(),nullable=False),
])
df1=spark.read.csv(r"C:\Users\Lakshmidevi\Downloads\task2.csv",inferSchema=True,schema=sc)
df1.show()

data=[("ram",56,'plm'),
      ("jan",24,'bng')]
df2=spark.createDataFrame(data=data,schema=sc)
df2.show()
p=df1.withColumn("age_n",col("age")**2).show()
#d=df1.join(df2,df2.age==df1.age,'inner').show()
#
# new=df1.join(df2,df2.age==df1.age,'inner').show()
# #left
# lf=df1.join(df2,df2.age==df1.age,'left').show()
#
# rg=df2.join(df1,df1.name==df2.name,'right').show()
#
# cross1=df1.join(df2,df2.name==df1.name,'cross').show()
# #semileft
# semil=df1.join(df2,df2.name==df1.name,'left_semi').show()
#
# #left anti
# la=df1.join(df2,df2.name==df1.name,'left_anti').show()
# ou=df1.join(df2,df2.name==df1.name,'outer').show()

#dr=df1.withColumn("new",lit("null")).show()

# d=df1.union(df2)
# d.show()
# d=df1.unionAll(df2).show()
# d=df1.unionByName(df2).show()

#df1.drop("new").show()
#handle null values
# res={"new":23}
# dr.fillna(res).show()

#drop duplicates
#df1.dropDuplicates(["name","age"]).show()

d=df1.join(df2,df2.name==df1.name,'inner').show()

df = df1.withColumn("new_col",lit("Lakshmi_devi"))
df3=df.withColumn("new_cols",split("new_col","_"))
df3.show()
df3=df3.withColumn("first_l",col('new_cols')[0]).withColumn("second_l",col('new_cols')[1])
df3.show()
df3=df.withColumn("new-cols",split('new_col',"_"))
df3=df3.withColumn("first_l",col("new-col")[0]).withColumn('last_l',col('new_col')[1])


# df3=df33.select("name",explode("new_cols")).alisa("new_arr")
# df3.show()

