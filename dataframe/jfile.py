from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("json Read Example") \
    .getOrCreate()

df=spark.read.json(r"C:\Users\Lakshmidevi\PycharmProjects\psyspark\pyspark_practice\dataframe\file1.json")

df.show()

# Filter out rows with null values in a specific column
df_filtered = df.filter(df['name'].isNotNull()).show()


df_filled = df.fillna({'name': 'ram'}).show()


#df.filter(df.name.isNotNull() & df.course.isNotNull() & df.id.isNotNull()).show()