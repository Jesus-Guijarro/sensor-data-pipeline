from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from datetime import timedelta

# Import ETL functions
from batch.extract import extract_sensor_readings
from batch.transform import transform_sensor_readings
from batch.load import load_reports

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sensor_batch_pipeline_dag',
    default_args=default_args,
    description='Sensors batch pipeline',
    schedule='1 0 * * *', 
    start_date=timezone.datetime(2025, 5, 8),
    catchup=False,
    tags=['etl', 'sensors'],
) as dag:

    @task()
    def extract(date_str: str):
        df = extract_sensor_readings(date_str)
        return df.to_json(date_format='iso', orient='split')

    @task()
    def transform(df_json: str):
        import pandas as pd
        df = pd.read_json(df_json, orient='split')

        reports = transform_sensor_readings(df)

        return reports

    @task()
    def load(reports: list):
        # Load transformed reports into MongoDB
        load_reports(reports)

    # Define task dependencies
    raw_data = extract(date_str="{{ data_interval_end | ds }}")
    processed_reports = transform(raw_data)
    load(processed_reports)
