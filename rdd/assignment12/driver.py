from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

def read_csv(spark, input_path):
    schema = StructType([
        StructField("data", StringType(), True)
    ])
    return spark.read.csv(input_path, header=False, schema=schema)

def mask_card_number(data):
    if len(data) >= 4:
        return "*" * (len(data) - 4) + data[-4:]
    else:
        return data

def log_partitions(df, message):
    logger = logging.getLogger(__name__)
    logger.info(f"{message}: {df.rdd.getNumPartitions()}")

# Create a Spark session
spark = SparkSession.builder \
    .appName("Partition Example") \
    .getOrCreate()

# Read CSV file
df = read_csv(spark, "input.csv")

# Define UDF for masking card number
mask_card_udf = udf(mask_card_number, StringType())

# Apply masking UDF
df = df.withColumn("masked_card_number", mask_card_udf("data"))

# Log initial number of partitions
log_partitions(df, "Initial partitions")

# Increase partitions
df = df.repartition(10)

# Log number of partitions after increasing
log_partitions(df, "Partitions after increasing")

# Reduce partitions
df = df.coalesce(5)

# Log number of partitions after reducing
log_partitions(df, "Partitions after reducing")
