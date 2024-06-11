from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the DAG
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG para ejecutar el spark job cada hora',
    schedule_interval='@hourly',
)

# Define bash command
base_dir = "/home/jfgs/Projects/sensor-data-pipeline"

jar_path = os.path.join(base_dir, 'postgresql-42.7.3.jar')
spark_job_path = os.path.join(base_dir, 'spark_job.py')

bash_command = f'spark-submit --jars {jar_path} {spark_job_path}'

# Define the BashOperator task to run the Spark job
spark_job_sensor = BashOperator(
    task_id='spark_job_sensor',
    bash_command=bash_command,
    dag=dag,
)

spark_job_sensor