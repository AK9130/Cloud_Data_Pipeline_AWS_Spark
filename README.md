# Cloud Data Pipeline using AWS (S3, EMR, Glue, Athena)

## Project Overview
This project demonstrates an **end-to-end cloud data pipeline** built on AWS using Apache Spark.
Raw accident data is stored in Amazon S3, processed using PySpark on EMR, cataloged using AWS Glue, and queried using Amazon Athena.

## Architecture
S3 (Raw Data) → EMR (Spark Processing) → S3 (Parquet Output) → Glue (Crawler & Data Catalog) → Athena (SQL Query)

## Dataset
- Name: US Accidents Dataset
- Size: ~2.9 GB
- Records: 7.7 million

## Project Structure
```
.
├── data_sets
│   └── raw_data
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
│   └── accidents_clean
│       ├── part-00000-26fe1585-5a20-41ab-9a40-045201e0b552-c000.snappy.parquet
│       ├── part-00001-26fe1585-5a20-41ab-9a40-045201e0b552-c000.snappy.parquet
│       ├── part-00021-26fe1585-5a20-41ab-9a40-045201e0b552-c000.snappy.parquet
│       ├── part-00022-26fe1585-5a20-41ab-9a40-045201e0b552-c000.snappy.parquet
│       └── _SUCCESS
├── queries
│   └── accident_analysis_queries.sql
├── README.md
└── scripts
    ├── emr_etl_job.py
    └── glue_etl_job.py
```
## Technologies Used

- AWS S3
- AWS EMR (Apache Spark)
- AWS Glue (Crawler & Data Catalog)
- AWS Athena
- PySpark
- Python
- Linux (Ubuntu)

---

## Data Pipeline Flow

### 1. Data Ingestion (S3)
- Uploaded raw dataset to Amazon S3

### 2. Data Processing (EMR)
- Created EMR cluster with Apache Spark
- Processed data using PySpark
- Cleaned and transformed dataset

### 3. Data Storage (S3)
- Stored processed data in **Parquet format**
- Partitioned by **state**

### 4. Data Cataloging (Glue)
- Created Glue Crawler
- Crawled S3 processed data
- Automatically generated table schema

### 5. Data Querying (Athena)
- Queried processed data using SQL
- Enabled serverless analytics

## Analysis Performed

- Top states with most accidents
- Accident severity distribution
- Weather condition analysis
- Top cities with most accidents
- Accidents by hour

## Screenshots

### S3
![S3 Upload](docs/screenshots/s3/raw_data.png)  
![S3 Output](docs/screenshots/s3/processed_data.png)

### EMR
![Cluster Setup](docs/screenshots/emr/setup/cluster.png)  
![Spark Execution](docs/screenshots/emr/execution/job.png)

### Glue
![Crawler Running](docs/screenshots/glue/crawler_running.png)  
![Crawler Success](docs/screenshots/glue/crawler_success.png)  
![Table Created](docs/screenshots/glue/table_created.png)  
![Schema](docs/screenshots/glue/table_schema.png)

### Athena
![Query Result](docs/screenshots/athena/query_result.png)
![Top States](docs/screenshots/athena/top_states.png)
![Severity Distribution](docs/screenshots/athena/severity.png)

## Sample Query

```sql
SELECT * FROM emr_state_wise_parquet LIMIT 10;
```

## Key Learnings

* Built an end-to-end AWS data pipeline
* Hands-on experience with EMR and Spark
* Understood Glue Crawlers and Data Catalog
* Queried large datasets using Athena

---

## Future Enhancements

* Load data into Amazon Redshift
* Build dashboards using BI tools
* Automate pipeline using workflows

---

## Conclusion

This project demonstrates how to build a scalable, serverless data pipeline using AWS services for big data processing and analytics.

