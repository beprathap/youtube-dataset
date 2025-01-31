from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflowYoutube import youtube_dataset_extractor

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="youtube_data_pipeline",  # Unique identifier for the DAG
    default_args=default_args,
    description="A DAG to extract and store YouTube data using Python scripts.",
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Extract YouTube data
    run_youtube_pipeline = PythonOperator(
        task_id="run_youtube_data_extraction",
        python_callable=youtube_dataset_extractor,
    )

    # No dependencies to define since there's only one task