# GCP Serverless Data Lake Pipeline

An incremental ELT pipeline that ingests data from PostgreSQL, processes it using DuckDB, and loads it into Google Cloud Storage with Hive-style partitioning.

## Tech Stack
* **Python** & **DuckDB** (Processing)
* **Google Cloud Storage** (Data Lake)
* **PostgreSQL** (Source System)
* **Docker** (Infrastructure)

## Features
* **Incremental Loading:** Uses High-Watermark strategy to process only new records.
* **Storage Optimization:** Partitions data by Year/Month/Day in Parquet format.
* **Security:** Uses Service Account authentication (excluded from repo).
