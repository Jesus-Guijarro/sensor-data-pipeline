from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG para ejecutar el spark job cada hora',
    schedule_interval='@hourly',
)

spark_job_sensor = BashOperator(
    task_id='spark_job_sensor',
    bash_command='spark-submit --jars /home/jfgs/Projects/sensor-data-pipeline/postgresql-42.7.3.jar /home/jfgs/Projects/sensor-data-pipeline/spark_job.py',
    dag=dag,
)

spark_job_sensor
