# Cloud Data Pipeline - US Accidents Dataset

## Project Overview
This project processes a large-scale US road accident dataset using Apache Spark.
The pipeline processes and analyzes accident data using AWS cloud services.

## Dataset
Dataset Name: US Accidents (2016–2023)

Size: 2.9 GB  
Rows: 7.7 million

Important Columns:
- ID
- Severity
- Start_Time
- City
- State
- Temperature(F)
- Weather_Condition

## Data Pipeline

Amazon S3 (Raw CSV)
↓
Amazon EMR (PySpark Processing)
↓
Data Cleaning & Analysis
↓
Amazon S3 (Parquet Output, partitioned by State)
↓
AWS Glue (Data Catalog)
↓
Amazon Athena (SQL Queries)

## Data Processing (PySpark)
- Read CSV from S3 without `inferSchema` (file scanned only once)
- Selected required columns and cast to proper types
  (`Severity` → int, `Start_Time` → timestamp, `Temperature(F)` → double)
- Renamed `Temperature(F)` to `Temperature_F`
- Dropped nulls in core columns using `dropna(subset=[...])`
- Printed row count before and after cleaning
- Cached cleaned data with `persist()`
- Used `repartition("State")` before `partitionBy("State")` to avoid many small files
- Used `coalesce(1)` for the small state-wise summary

## Analysis Performed
1. Top states with most accidents
2. Accident severity distribution
3. Accidents by weather condition
4. Top cities with most accidents
5. Accidents by hour (time analysis)
6. Month-wise accidents (Athena)
7. Top 5 states by average severity (Athena)
8. Top 3 cities per state using window function (Athena)

## Tools Used
- AWS S3
- AWS EMR (Apache Spark)
- AWS Glue (Crawler & Data Catalog)
- AWS Athena
- PySpark
- Python
- Linux (Ubuntu)

For full details, see the main [README](../README.md).
