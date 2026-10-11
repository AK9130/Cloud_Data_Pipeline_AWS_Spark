from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import year, avg, to_timestamp, col

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet("s3://aaqib-data-pipeline-bucket/output/accidents_clean_parquet")

# Fix columns
df = df.withColumnRenamed("Temperature(F)", "Temperature_F")
df = df.withColumnRenamed("Visibility(mi)", "Visibility_mi")

# Transform
df = df.withColumn("Start_Time", to_timestamp(col("Start_Time")))

# Aggregation
severity_df = df.groupBy("Severity").count()

# Save output
severity_df.write \
  .mode("overwrite") \
  .parquet("s3://aaqib-data-pipeline-bucket/output/processed_severity_accidents_parquet/")
