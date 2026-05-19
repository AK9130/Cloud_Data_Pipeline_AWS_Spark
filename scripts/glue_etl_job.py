from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import year, avg, to_timestamp, col

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet("s3://aaqib-data-pipeline-bucket/output/emr_state_wise_parquet/")

# Fix columns
df = df.withColumnRenamed("Temperature(F)", "Temperature_F")
df = df.withColumnRenamed("Visibility(mi)", "Visibility_mi")

# Transform
df = df.withColumn("Start_Time", to_timestamp(col("Start_Time")))

# Aggregation
state_df = df.groupBy("State").count()

# Save output
state_df.write \
  .mode("overwrite") \
  .parquet("s3://aaqib-data-pipeline-bucket/output/processed_state_accidents_parquet/")
