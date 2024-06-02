from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark_job():
    subprocess.run(["spark-submit", "--master", "local", "/home/jfgs/Projects/sensor-data-pipeline/spark_job.py"])

with DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A simple Spark job DAG',
    schedule_interval=timedelta(hours=1)#timedelta(days=1), #timedelta(hours=1)
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    run_spark = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
    )

run_spark
