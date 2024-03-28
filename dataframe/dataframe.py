from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Example").getOrCreate()

csv_path = "C:/Users/Lakshmidevi/PycharmProjects/psyspark/pyspark_practice/dataframe/first2.csv"
options = {
    "header": "true",

    "inferSchema": "true"
}

# Read the CSV file with options
df = spark.read.csv(csv_path, **options)

df = df.withColumn("price", lit(20000))
df=df.withColumn("Price",when(col("customer")==1,3000).when(col("customer")==2,4000).otherwise(col("price")))
#df.show()
df=df.withColumn("date",lit("None"))

df=df.drop('date')
#df=df.select("customer")
df=df.withColumnRenamed("price","cost")
df1=df.sort("customer",ascending=[True, False])

#df.filter(col("productm_mode"))
df.printSchema()
#df.show()
df.filter((col("product_model").contains("i5"))).show()
df.filter((col("product_model").like('h%'))).show()