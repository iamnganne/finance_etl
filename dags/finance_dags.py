from airflow import DAG
from datetime import datetime, timedelta 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2)
}
with DAG( 
    dag_id = "finance_etle_pipeline",
    default_args = default_args,
    description='ETL pipeline for financial data using PySpark and PostgreSQL',
    schedule_interval='@daily',
    start_date = datetime(2024,1,1)
    tag = ['finance','etl','spark'],
) as dag:
    extract_task = SparkSubmitOperator (
        task_id = 'extract_data_yahoofinance',
        application = "/opt/airflow/spark_jobs/extract_data.py",
        conn_id = "spark-conn",
        verbose = False,
        name = "extract_data_yahoofinance"
    )
    transform_task = SparkSubmitOperator (
        task_id = 'tranform_data',
        application = "/opt/airflow/spark_jobs/transform_data.py",
        conn_id = "spark-conn",
        verbose = False,
        name = "transform_data"
    )
    
    load_task = SparkSubmitOperator (
        task_id = 'load_data',
        application = "/opt/airflow/spark_jobs/load_data.py",
        conn_id = "spark-conn",
        verbose = False,
        name = "load_data"
    )

    extract_task >> transform_task >> load_task