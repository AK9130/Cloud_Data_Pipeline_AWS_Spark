# Cloud Data Pipeline Using AWS and Apache Spark

## 1. Project Overview
This project demonstrates an end-to-end cloud data pipeline built with AWS and Apache Spark to process and analyze the US Accidents dataset.
The pipeline stores raw data in Amazon S3, processes it with PySpark on Amazon EMR, writes Parquet outputs to S3, uses AWS Glue for data cataloging, and queries cataloged data with Amazon Athena.

## 2. Problem Statement
The US Accidents dataset contains millions of records with details such as accident severity, location, start time, temperature, and weather conditions.

The objective is to build a cloud-based pipeline that can:

- Store raw data in Amazon S3.
- Process large-scale data using PySpark on Amazon EMR.
- Select required columns and remove records containing null values.
- Analyze accident records by state, severity, weather, city, and hour.
- Store processed data in Parquet format, partitioned by state where applicable.
- Use AWS Glue to catalog processed data.
- Run SQL analysis using Amazon Athena.

## 3. Dataset

**Dataset:** US Accidents (March 2023 version)
**Local path:** `data_sets/accidents_data/US_Accidents_March23.csv`  
**Approximate size:** 2.9 GB  
**Approximate records:** 7.7 million

Columns used in the project include:

- `ID`
- `Severity`
- `Start_Time`
- `City`
- `State`
- `Temperature(F)`
- `Weather_Condition`

## 4. Technologies Used

- Python
- Apache Spark / PySpark
- Amazon S3
- Amazon EMR
- AWS Glue Crawler and Data Catalog
- Amazon Athena
- Parquet
- Linux (Ubuntu)

## 5. Pipeline Architecture

```text
US Accidents CSV
       |
       v
Amazon S3 (Raw Data)
       |
       v
Amazon EMR + PySpark
       |
       v
Data Cleaning and Analysis
       |
       v
Partitioned Parquet Output (by State)
       |
       v
Processed Parquet Output in S3
       |
       v
AWS Glue Crawler
       |
       v
Glue Data Catalog
       |
       v
Amazon Athena (SQL Analysis)
```

## 6. Implementation
### Step 1: Upload the Raw Dataset to S3
Upload the dataset to the configured S3 bucket:

```text
s3://aaqib-data-pipeline-bucket/data_sets/accidents_data/US_Accidents_March23.csv
```

### Step 2: Process Data Using Amazon EMR
Run the PySpark job:

```text
scripts/emr_etl_job.py
```

The job reads the CSV file from S3, selects the required columns, removes records containing null values, and performs the following analyses:
The cleaned data is written in Parquet format, partitioned by `State`, to:

```text
s3://aaqib-data-pipeline-bucket/output/emr_state_wise_parquet/
```

### Step 3: Catalog Data with AWS Glue
Use an AWS Glue Crawler to crawl the intended processed S3 output location and create or update the table schema in the Glue Data Catalog.

### Step 4: Query Data with Amazon Athena

Run SQL queries against the cataloged tables using the query file:

```text
queries/accident_analysis_queries.sql
```
## 7. Analysis Performed

### State-wise Accident Analysis
Counts accident records by state and helps identify states with the highest number of records.

### Severity Analysis
Counts accident records for each severity level.

### Weather-condition Analysis
Analyzes accident records by weather condition.

### City-wise Analysis
Identifies cities with the highest number of accident records.

### Hourly Analysis
Analyzes accident activity using the hour extracted from `Start_Time`.

### Additional Athena Queries
The SQL query file also includes

 Top 10 states with the most accidents
- Accidents by severity
- Top 10 weather conditions with the most accidents
- Top 10 cities with the most accidents
- Accidents per hour
- Top 10 accident counts per minute
- Top 5 states with the highest average severity
- Top 3 cities per state using CTE and Window Function

## 8. Project Structure

```text
.
├── data_sets/
│   └── accidents_data/
│       └── US_Accidents_March23.csv
├── docs/
│   ├── project_documentation.md
│   └── screenshots/
│       ├── athena/
│       ├── emr/
│       │   ├── execution/
│       │   └── setup/
│       ├── glue/
│       └── s3/
├── output/
│   ├── accidents_clean_parquet/
│   ├── emr_state_wise_parquet/
│   └── athena_results/
├── queries/
│   └── accident_analysis_queries.sql
├── scripts/
│   ├── emr_etl_job.py
│  
└── README.md
```

The `output/` folders contain local Parquet outputs. Generated part-file names can vary between runs, so they are not listed individually here.

## 9. Outputs and Screenshots
Local Parquet output folders documented in this repository:

```text
output/accidents_clean_parquet/
output/emr_state_wise_parquet/
output/athena_results/
```

The project also writes processed data to Amazon S3. Screenshots documenting the AWS workflow are stored under:

```text
docs/screenshots/
```

They include screenshots related to S3, EMR setup and execution, Glue crawler/cataloging, and Athena query results.

## 10. Challenges and Learning
### Challenges

- Processing a large CSV dataset with Spark.
- Reading raw data directly from Amazon S3.
- Cleaning records before analysis.
- Writing Parquet output and partitioning data by state.
- Cataloging S3 data using AWS Glue.
- Querying cataloged data using Amazon Athena.

### Key Learnings

- Building an AWS-based data pipeline.
- Using Amazon S3 for raw and processed data.
- Processing large datasets with PySpark on Amazon EMR.
- Writing Parquet data and partitioning by state.
- Using AWS Glue Crawlers and the Data Catalog.
- Running SQL analysis with Amazon Athena.

## 11. My Contribution
I worked on the project workflow, including uploading the raw dataset to S3, processing it with PySpark on EMR, selecting required columns, removing null records, performing accident analysis, writing Parquet outputs, using AWS Glue Crawler and Data Catalog for cataloging, and preparing Athena SQL queries.

## 12. How to Run
1. **Upload the raw CSV** to the S3 location configured for the project.
2. **Submit `scripts/emr_etl_job.py`** to an appropriately configured EMR Spark environment.
3. **Run the Glue Crawler** against the intended output location to create or update the Data Catalog table.
4. **Execute the SQL statements** in `queries/accident_analysis_queries.sql` in Athena after confirming the database, table, and S3 location.

## 13. Conclusion
This project demonstrates a cloud data pipeline for processing and analyzing the US Accidents dataset with PySpark and AWS services. It covers raw-data storage in S3, distributed processing with EMR, Parquet outputs, Glue-based cataloging, and SQL analysis in Athena.
