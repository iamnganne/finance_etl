# Build Finance ETL Pipeline with Pyspark, Airflow 
## Features
- Extract stock data from Yahoo Finance
- Transform data (using Pyspark): clean missing value, calculate log returns, moving average, ...
- Load cleaned data into PostgreSQL databse
- Schedule and monitor workflows via Apache Airflow
- Fully containerized with Docker
## Project Structure 
          finance-etl-pipeline/
          ├── dags/ ← Airflow DAG definitions
          ├── spark_jobs/ ← Modular ETL pipeline
          │ ├── extract_data/ ← Data extraction logic
          │ ├── transform_data/ ← Data transformation with PySpark
          │ └── load_data/ ← Load logic into PostgreSQL
          ├── config/ ← Config files (DB, API, Airflow)
          ├── scripts/ ← Wrapper scripts (for Airflow)
          ├── data/ ← Raw and processed data
          ├── tests/ ← Unit tests for ETL
          ├── requirements.txt ← Python dependencies
          ├── docker-compose.yml ← Docker setup for Airflow + PostgreSQL
          └── README.md ← Project overview
## Technology 
- Pyspark
- Apache Airflow
- PostgreSQL
- Docker Compose
- Yahoo Finance API         
## Quick Start 
1. Clone the repository
  ```bash
  $ git clone https://github.com/iamnganne/finance_etl.git
  ```
 ```bash
  cd finance-etl-pipeline
  ```
2. Start Docker containers
  ```bash
  docker-compose up -d
  ```
3. Access Airflow
  URL: http://localhost:8080
  Default login:
    Username: airflow
    Password: airflow
. Run pipeline manually
