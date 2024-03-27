from pyspark.sql.types import StructType,StructField,StringType
schema= StructType([
    StructField("name",StringType(),True),
    StructField("age", StringType(),True),
    StructField("city",StringType(),True)
])



