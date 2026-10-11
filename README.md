ROJECTS/6_Cloud_Data_Pipeline_AWS_Spark$ cat README.md
# Cloud Data Pipeline Using AWS and Spark

## 1. Project Introduction
This project demonstrates an end-to-end cloud data pipeline using AWS and Apache Spark to process and analyze the US Accidents dataset.
The pipeline stores raw accident data in Amazon S3, processes the data using PySpark on Amazon EMR, stores the processed data in Parquet format, catalogs the data using AWS Glue, and queries the processed data using Amazon Athena.

## 2. Problem Statement
The US Accidents dataset contains millions of accident records with information about severity, location, time, weather, and other attributes.

The objective of this project is to build a cloud-based data pipeline that can:

- Store raw data in Amazon S3
- Process large-scale data using PySpark on Amazon EMR
- Clean and transform the required data
- Store processed data in partitioned Parquet format
- Catalog the processed data using AWS Glue
- Analyze the data using Amazon Athena

## 3. Dataset
The dataset used in this project is the US Accidents dataset covering 2016–2023.

**Dataset file:**

```text
data_sets/accidents_data/US_Accidents_March23.csv
```

According to the project documentation:

- Size: approximately 2.9 GB
- Records: approximately 7.7 million

Important columns include:

- `ID`
- `Severity`
- `Start_Time`
- `City`
- `State`
- `Temperature(F)`
- `Weather_Condition`

## 4. Technologies Used
- Amazon S3
- Amazon EMR
- Apache Spark
- PySpark
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- Python
- Linux (Ubuntu)
- Parquet

## 5. Project Architecture / Flow
```text
US Accidents CSV
       |
       v
   Amazon S3
   Raw Data
       |
       v
Amazon EMR
PySpark Processing
       |
       v
Data Cleaning
       |
       v
Analysis
       |
       v
Partitioned Parquet
   by State
       |
       v
   Amazon S3
       |
       v
AWS Glue Crawler
       |
       v
Glue Data Catalog
       |
       v
Amazon Athena
       |
       v
    SQL Analysis
```

## 6. Implementation / Processing
The project uses two PySpark-based scripts for cloud processing.

### Step 1: Store Raw Data in S3
The raw US Accidents CSV dataset is uploaded to an Amazon S3 bucket.

### Step 2: Process Data Using EMR
The EMR PySpark job reads the CSV data directly from S3:

```text
s3://aaqib-data-pipeline-bucket/data_sets/accidents_data/US_Accidents_March23.csv
```

The script selects the required columns and removes records containing null values.

Selected columns include:

- `ID`
- `Severity`
- `Start_Time`
- `City`
- `State`
- `Temperature(F)`
- `Weather_Condition`

### Step 3: Perform Analysis
The EMR Spark job performs:

- State-wise accident analysis
- Severity analysis
- Weather condition analysis
- City-wise accident analysis
- Hourly accident analysis

### Step 4: Store Processed Data
The cleaned data is written to S3 in Parquet format and partitioned by `State`:

```text
s3://aaqib-data-pipeline-bucket/output/emr_state_wise_parquet/
```

### Step 5: Process Using AWS Glue
The Glue PySpark job reads the processed clean Parquet data from S3.

It:

- Renames selected columns
- Converts `Start_Time` to timestamp format
- Performs severity-wise aggregation
- Writes the processed severity-level results back to S3

Output location:

```text
s3://aaqib-data-pipeline-bucket/output/processed_state_accidents_parquet/
```

### Step 6: Catalog Data Using AWS Glue
An AWS Glue Crawler is used to crawl the processed S3 data and create the table schema in the Glue Data Catalog.

### Step 7: Query Using Amazon Athena
Amazon Athena is used to run SQL queries against the cataloged data.

## 7. My Role
I worked on the complete cloud data pipeline, including:

- Prepared the US Accidents dataset for cloud processing.
- Uploaded raw data to Amazon S3.
- Worked with Amazon EMR and PySpark for data processing.
- Selected required columns from the raw dataset.
- Removed null records.
- Performed state, severity, weather, city, and hourly analysis.
- Stored processed data in partitioned Parquet format on S3.
- Worked with AWS Glue for data cataloging.
- Processed the Parquet data using the Glue ETL job.
- Performed SQL-based analysis using Amazon Athena.

## 8. Analysis Performed

### Top States with Most Accidents
Counts accidents by state and identifies the states with the highest number of accident records.

### Accident Severity Analysis
Counts accident records for each severity level.

### Weather Condition Analysis
Analyzes accident records by weather condition.

### City-wise Accident Analysis
Identifies cities with the highest number of accident records.

### Accidents by Hour
Analyzes accident activity based on the hour extracted from `Start_Time`.

### Additional Athena Analysis
The SQL query file also contains:

- Accident count per minute
- States with highest average severity
- Top 3 cities per state using a window function

The queries are available in:

```text
queries/accident_analysis_queries.sql
```

## 9. Output / Results
The project contains locally generated Parquet output under:

```text
output/accidents_clean_parquet/
```

The project also stores processed Parquet data in Amazon S3.

The EMR processing output is partitioned by state.

Screenshots documenting the AWS workflow are available under:

```text
docs/screenshots/
```

They include screenshots for:

- Amazon S3
- Amazon EMR
- AWS Glue
- Amazon Athena

## 10. Challenges & Learning
### Challenges
- Processing a large accident dataset using distributed Spark processing.
- Reading large CSV data directly from Amazon S3.
- Cleaning the required data before analysis.
- Writing partitioned Parquet output to S3.
- Cataloging processed data using AWS Glue.
- Querying the cataloged data using Amazon Athena.

### Learning
Through this project, I gained practical experience in:

- Building an AWS-based data pipeline
- Working with Amazon S3
- Processing data using Amazon EMR and PySpark
- Writing Parquet output
- Partitioning data by state
- Working with AWS Glue Crawlers and Data Catalog
- Running SQL queries using Amazon Athena
- Processing large-scale datasets using Spark

## 11. Project Structure
```text
.
├── data_sets
│   └── accidents_data
│       └── US_Accidents_March23.csv
├── docs
│   ├── project_documentation.md
│   └── screenshots
│       ├── athena
│       │   ├── query_result.png
│       │   ├── severity.png
│       │   └── top_states.png
│       ├── emr
│       │   ├── execution
│       │   │   ├── emr_city_analysis.png
│       │   │   ├── emr_df_show.png
│       │   │   ├── emr_hour_analysis.png
│       │   │   ├── emr_s3_partition_output.png
│       │   │   ├── emr_severity_analysis.png
│       │   │   ├── emr_state_analysis.png
│       │   │   └── emr_weather_analysis.png
│       │   └── setup
│       │       ├── emr_cluster_running.png
│       │       ├── emr_cluster_starting.png
│       │       ├── emr_create_cluster.png
│       │       ├── emr_inbound_rules.png
│       │       ├── emr_instance_config.png
│       │       ├── emr_networking.png
│       │       ├── emr_network_security_iam.png
│       │       ├── emr_security_iam.png
│       │       └── emr_ssh_connected.png
│       ├── glue
│       │   ├── crawler_running.png
│       │   ├── crawler_success.png
│       │   ├── table_created.png
│       │   └── table_schema.png
│       └── s3
│           ├── s3_bucket_overview.png
│           ├── s3_data_sets_folder.png
│           ├── s3_output.png
│           └── s3_raw_data_files.png
├── output
│   ├── accidents_clean_parquet
│   │   ├── part-00000-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│   │   ├── part-00001-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│   │   ├── part-00002-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│   │   ├── part-00003-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│   │   ├── part-00004-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│   │   ├── part-00005-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet 
│   │   └── _SUCCESS
│   ├── emr_state_wise_parquet
│   │   ├── part-00000-b4647951-3cf5-44b8-a556-f5f3ce6c1a1c-c000.snappy.parquet
│   │   ├── part-00001-b4647951-3cf5-44b8-a556-f5f3ce6c1a1c-c000.snappy.parquet
│   │   ├── part-00002-b4647951-3cf5-44b8-a556-f5f3ce6c1a1c-c000.snappy.parquet
│   │   ├── part-00003-b4647951-3cf5-44b8-a556-f5f3ce6c1a1c-c000.snappy.parquet
│   │   ├── part-00004-b4647951-3cf5-44b8-a556-f5f3ce6c1a1c-c000.snappy.parquet
│   │   └── _SUCCESS
│   └── processed_severity_accident_parquet
│       ├── part-00000-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       ├── part-00001-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       ├── part-00002-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       ├── part-00003-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       ├── part-00004-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       ├── part-00005-1559cc4e-cb09-4667-8b62-609e0dfadabe.c000.snappy.parquet
│       └── _SUCCESS
├── queries
│   └── accident_analysis_queries.sql
├── README.md
└── scripts
    ├── emr_etl_job.py
    └── glue_etl_job.py
```

## 12. How to Run
### Step 1: Upload Raw Data to S3
Upload:

```text
data_sets/accidents_data/US_Accidents_March23.csv
```

to the configured S3 raw-data location.

### Step 2: Run the EMR PySpark Job
Use:
```text
scripts/emr_etl_job.py
```

The job reads the raw data from S3, cleans and analyzes it, and writes partitioned Parquet data to S3.

### Step 3: Run the Glue ETL Job
Use:
```text
scripts/glue_etl_job.py
```

The job reads the EMR Parquet output, performs additional transformation and aggregation, and writes the processed result to S3.

### Step 4: Run Athena Queries
Use:
```text
queries/accident_analysis_queries.sql
```

## 13. Conclusion
This project demonstrates an end-to-end cloud data pipeline for processing large-scale accident data using AWS and Apache Spark.
The project provided practical experience in Amazon S3 data storage, EMR-based Spark processing, partitioned Parquet storage, AWS Glue >

