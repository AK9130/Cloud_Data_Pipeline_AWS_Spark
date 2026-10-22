from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

spark = SparkSession.builder.appName("AccidentDataProcessing").getOrCreate()

# S3 data read
df = spark.read.csv(
    "s3://aaqib-data-pipeline-bucket/data_sets/accidents_data/US_Accidents_March23.csv",
    header=True,
)

# schema check
df.printSchema()

# show sample
df.show(5)

# before
print(df.count())


# select useful columns + clean
df_clean = df.select(
    "ID",
    F.col("Severity").cast("int").alias("Severity"),
    F.to_timestamp("Start_Time").alias("Start_Time"),
    "City", "State",
    F.col("Temperature(F)").cast("double").alias("Temperature_F"),
    "Weather_Condition",
)

df_clean = df_clean.dropna(subset=["ID", "Severity", "Start_Time", "State", "City"])

df_clean.persist(StorageLevel.MEMORY_AND_DISK)

# after
print(df_clean.count())

df.show()

# analysis

# 1. State analysis (top 10)
df_clean.groupBy("State").count() \
    .orderBy("count", ascending=False).show(10)

# 2. Severity analysis
df_clean.groupBy("Severity").count() \
    .orderBy("Severity").show()

# 3. Weather analysis
df_clean.filter(F.col("Weather_Condition").isNotNull()) \
    .groupBy("Weather_Condition").count() \
    .orderBy("count", ascending=False).show(10)

# 4. City analysis
df_clean.groupBy("City").count() \
    .orderBy("count", ascending=False).show(10)

# 5. Hour analysis
df_clean.groupBy(F.hour("Start_Time").alias("Hour")).count() \
    .orderBy("Hour").show(24)

# 6. Save cleaned data partitioned by State
df_clean.repartition("State").write.mode("overwrite").partitionBy("State").parquet(
    "s3://aaqib-data-pipeline-bucket/output/accidents_clean_parquet/"
)

# 7. State-wise accident count analysis (saare states)
df_state = df_clean.groupBy("State").count() \
    .orderBy("count", ascending=False)

df_state.show(50, truncate=False)

# 8. Save state-wise counts (chhota data, partition nahi)
df_state.coalesce(1).write.mode("overwrite").parquet(
    "s3://aaqib-data-pipeline-bucket/output/emr_state_wise_parquet/"
)

df_clean.unpersist()
spark.stop()
