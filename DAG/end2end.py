from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    }

# Define the DAG
with DAG(
    default_args = default_args,
    dag_id = "project_e2e",
    description = "DAG for Hive",
    start_date = datetime(2023, 5, 23),
    schedule = '@daily' # Runs daily at midnight
) as dag:
    # Task 1
    task_1 = BashOperator(
        task_id='collect_data',
        bash_command='python3 /home/mohammadarif/cap/download_file.py'
    )    
    # Task 2
    task_2 = BashOperator(
        task_id='read_and_transform',
        bash_command='python3 /home/mohammadarif/cap/read_and_transformation.py'
    )
    # Task 3    
    task_3 = BashOperator(
        task_id='save_tables',
        bash_command='python3 /home/mohammadarif/cap/save_tables.py'
    )
    task_1 >> task_2 >> task_3
