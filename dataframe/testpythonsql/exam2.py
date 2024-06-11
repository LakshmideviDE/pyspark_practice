# def remove_vowels(str):
#     vowel= ('a','e','i','o','u','A','E','I','O','U')
#     return ''.join(i for i in str if i not in vowel)
# str = "Gaurav"
# str = remove_vowels(str)
# print(str)

# def remove_v(str):
#     v = ('a','e','i','o','u','A','E','I','O','U')
#     return ''.join(filter(lambda y: y not in v, str))
# str = "Gaurav"
# res= remove_v(str)
# print(res)

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("finding customer data").getOrCreate()

data1=[(1,"Joe"),(2,"Henry"),(3,"sam"),(4,"max")]
data2=[(1,3),(2,1)]
schema1=["id","NameCust"]
schema2=["id","customer_id"]

df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
def cus(id, customer_id):
    return id == customer_id

filter_udf = udf(cus, StringType())

res = df1.join(df2, df1["id"]==df2["Customer_id"], "left_anti") \
         .select(df1["NameCust"].alias("Customers"))
res.show()






