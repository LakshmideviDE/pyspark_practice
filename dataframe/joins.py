from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("simple joins").getOrCreate()
data1=[("chandu",101,"MCA"),
       ("Dina",102,"B.tech"),
       ("David",103,"MSC"),
       ("lakshmi",104,"BCA")]

data2=[("rani",1,"MCA"),
       ("Dina",2,"B.tech"),
       ("Jan",3,"BSC"),
       ("rup",4,"CA")]
schema=["name","id","course"]
df1=spark.createDataFrame(data1,schema)
df2=spark.createDataFrame(data2,schema)
df1.show()
df2.show()
#df=df1.join(df2,"course","left")
#df.show()
#df=df1.join(df2,"name","right")
df=df1.join(df2,"name","inner")
df.show()
df=df1.join(df2,"name","cross")
df.show()