# Cloud Data Pipeline - US Accidents Dataset

## Project Overview
This project processes a large-scale US road accident dataset using Apache Spark. 
The pipeline processes and analyzes accident data using AWS cloud services.

## Dataset
Dataset Name: US Accidents (2016–2023)

Size: ~2.9 GB  
Rows: ~7.7 million

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
Amazon S3 (Parquet Output)
↓
AWS Glue (Data Catalog)
↓
Amazon Athena (SQL Queries)

## Analysis Performed
1. Top states with most accidents
2. Accident severity distribution
3. Accidents by weather condition
4. Top cities with most accidents
5. Accidents by hour (time analysis)

## Tools Used
- AWS S3
- AWS EMR (Apache Spark)
- AWS Glue (Crawler & Data Catalog)
- AWS Athena
- PySpark
- Python
- Linux (Ubuntu)
