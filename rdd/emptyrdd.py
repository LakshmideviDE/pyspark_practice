# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("empty rdd creating").getOrCreate()
# emptyRDD=spark.sparkContext.emptyRDD()
# print(emptyRDD)
#
# #creating Rdd using parallelize
# rdd1=spark.sparkContext.parallelize([])
# print(rdd1)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

# Sample DataFrame
# data = [("John", 25), ("Alice", 30), ("Bob", 35)]
# df = spark.createDataFrame(data, ["Name", "Age"])
#
# # Define a Python function
# def greet(name):
#     return "Hello, " + name
#
# # Register the Python function as a UDF
# greet_udf = udf(greet, StringType())
#
# # Apply the UDF to the DataFrame
# df = df.withColumn("Greeting", greet_udf(df["Name"]))
#
# # Show the result
# df.show()
#
# # Stop the SparkSession
# spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Operations") \
    .getOrCreate()

# Sample data
data = [("John", 30, "New York"),
        ("Alice", 25, "Los Angeles"),
        ("Bob", 35, "Chicago"),
        ("Alice", 25, "Los Angeles")]

# Define schema
schema = ["name", "age", "city"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
print("Original DataFrame:")
df.show()

# Select Columns
selected_df = df.select("name", "age")
print("Selected Columns:")
selected_df.show()

# Add and Update Column (withColumn)
updated_df = df.withColumn("age_plus_5", col("age") + 5)
print("DataFrame with updated column:")
updated_df.show()

# Rename Nested Column
renamed_df = df.withColumnRenamed("name", "full_name")
print("DataFrame with renamed column:")
renamed_df.show()

# Drop column
dropped_df = df.drop("city")
print("DataFrame with dropped column:")
dropped_df.show()

# Where | Filter (and , or)
filtered_df = df.filter((col("age") > 25) & (col("city") == "New York"))
print("Filtered DataFrame:")
filtered_df.show()

# When Otherwise
updated_df_with_condition = df.withColumn("age_category",
                                          when(col("age") < 30, "Young").otherwise("Old"))
print("DataFrame with conditional column:")
updated_df_with_condition.show()

# Drop Duplicates
deduplicated_df = df.dropDuplicates()
print("DataFrame after dropping duplicates:")
deduplicated_df.show()

# Distinct
distinct_df = df.select("city").distinct()
print("Distinct values in city column:")
distinct_df.show()

# Groupby in pyspark
grouped_df = df.groupBy("city").count()
print("Grouped DataFrame:")
grouped_df.show()
