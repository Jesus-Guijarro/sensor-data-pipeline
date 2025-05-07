from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.sensor_temperatures import run
from database import get_connection

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sensor_temperatures_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "sensor"],
) as dag:

    def etl_task():
        src = get_connection("warehouse")
        dst = get_connection("analytics")
        run(src, dst)

    PythonOperator(
        task_id="extract_transform_load",
        python_callable=etl_task,
    )
