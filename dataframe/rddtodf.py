from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("creating of data").getOrCreate()

df = spark.read.csv(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\first2.csv", header=True,
                    inferSchema=True)
# df.show()
# df.select(df.columns[:2]).show(3)
df.printSchema()
# df.select("*").show()
# df.show(2,truncate=2)
# df1.show()

#df1 = df.select("product_id")
#df1.show()
#df1=df.collect()
#print(df1)
#df1=df.collect()[0]
#print(df1)
rows=df.count()
print(rows)
#cols=len(df.columns)
#print(cols)

df.show()
df.printSchema()

df = spark.read.parquet(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\sec.parquet")
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("creating of data").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.format("csv") \
               .option("header", "true") \
               .option("inferSchema", "true") \
               .load(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\first2.csv")

# Print the schema
df.printSchema()
df.select(translate('product_model',"A","#").alias('translate_col'),df.product_model).show()
df.select(length(df.product_model).alias('product_model'), df.product_model).show()
