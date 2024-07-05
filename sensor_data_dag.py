from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'jesus-guijarro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the folder path where the scripts are located
folder_path = '/home/jfgs/Projects/sensor-data-pipeline'

# Create the DAG
with DAG(
    'sensor_data_pipeline',
    default_args=default_args,
    description='A DAG to run sensor data scripts',
    schedule_interval='0 20 * * *',  # This sets the DAG to run daily at 20:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task to run sensor_etl_job.py
    run_sensor_etl = BashOperator(
        task_id='run_sensor_etl',
        bash_command=f'cd {folder_path} && python sensor_etl_job.py',
        execution_timeout=timedelta(minutes=5),
        dag=dag,
    )

    # Set task dependencies
    run_sensor_etl