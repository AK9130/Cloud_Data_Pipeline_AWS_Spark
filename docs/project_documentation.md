# Cloud Data Pipeline - US Accidents Dataset

## Project Overview
This project processes a large-scale US road accident dataset using Apache Spark. 
The goal is to clean, transform, and analyze accident data locally before deploying the pipeline to AWS cloud services.

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

Raw Data (CSV)
↓
Spark Processing
↓
Data Cleaning
↓
Analysis
↓
Parquet Output

## Analysis Performed
1. Top states with most accidents
2. Accident severity distribution
3. Accidents by weather condition
4. Top cities with most accidents
5. Accidents by hour (time analysis)

## Tools Used
- Apache Spark
- Python (PySpark)
- Linux (Ubuntu)

## Future Cloud Integration
The processed data will be uploaded to AWS services:
- Amazon S3
- AWS Glue
- Amazon Athena
- Amazon Redshift
