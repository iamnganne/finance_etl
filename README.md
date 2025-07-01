# Build Finance ETL Pipeline with Pyspark, Airflow 
## Features
This project aim to buil ETL Pipline include: 
- Extract stock data from Yahoo Finance
- Transform data (using Pyspark): clean missing value, calculate log returns, moving average, ...
- Load cleaned data into PostgreSQL databse
- Schedule and monitor workflows via Apache Airflow
- Fully containerized with Docker
## Project Structure 
          finance-etl-pipeline/
          ├── dags/ 
          ├── spark_jobs/
          │ ├── extract_data/ 
          │ ├── transform_data/ 
          │ └── load_data/ 
          ├── config/ 
          ├── scripts/ 
          ├── data/ 
          │ ├── raw/ 
          │ ├── processed/ 
          ├── requirements.txt 
          ├── docker-compose.yml
          └── README.md 
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
```bash
   URL: http://localhost:8080
   Default login:
    Username: airflow
    Password: airflow
   ```
4. Run pipeline manually
