from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

spark = SparkSession.builder.appName("AccidentDataProcessing").getOrCreate()

# S3 se data read
df = spark.read.csv(
    "s3://aaqib-data-pipeline-bucket/data_sets/raw_data/US_Accidents_March23.csv",
    header=True,
    inferSchema=True
)

# schema check
df.printSchema()

# show sample
df.show(5)

# select useful columns
df_clean = df.select(
    "ID",
    "Severity",
    "Start_Time",
    "City",
    "State",
    "Temperature(F)",
    "Weather_Condition"
)

# remove nulls
df_clean = df_clean.dropna()

# analysis

# 1. State analysis
df_clean.groupBy("State").count() \
.orderBy("count", ascending=False).show(10)

# 2. Severity analysis
df_clean.groupBy("Severity").count() \
.orderBy("Severity").show()

# 3. Weather analysis
df_clean.groupBy("Weather_Condition").count() \
.orderBy("count", ascending=False).show(10)

# 4. City analysis
df_clean.groupBy("City").count() \
.orderBy("count", ascending=False).show(10)

# 5. Hour analysis
df_clean.groupBy(hour("Start_Time").alias("Hour")).count() \
.orderBy("Hour").show(24)

df_clean.write.mode("overwrite").partitionBy("State").parquet(
    "s3://aaqib-data-pipeline-bucket/output/emr_state_wise_parquet/"
)

spark.stop()
