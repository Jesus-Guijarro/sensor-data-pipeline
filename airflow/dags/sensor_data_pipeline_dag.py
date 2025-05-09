from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

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

# DAG start date: one day before definition time
start_date = datetime.now() - timedelta(days=1)

with DAG(
    dag_id='sensor_batch_pipeline_dag',
    default_args=default_args,
    description='Sensors ETL pipeline: extract -> transform -> load',
    schedule='0 0 * * *', 
    start_date=start_date,
    catchup=False,
    tags=['etl', 'sensors'],
) as dag:

    @task()
    def extract():
        # Previous full day window: 00:00 to 23:55 UTC
        yesterday = datetime.now() - timedelta(days=1)
        start_ts = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_ts = yesterday.replace(hour=23, minute=55, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        df = extract_sensor_readings(start_ts, end_ts)
        # Serialize DataFrame to JSON for XCom
        return df.to_json(date_format='iso', orient='split')

    @task()
    def transform(df_json: str):
        import pandas as pd
        df = pd.read_json(df_json, orient='split')
        # Apply transformation logic to sensor readings
        reports = transform_sensor_readings(df)
        # Return list of report dictionaries
        return reports

    @task()
    def load(reports: list):
        # Load transformed reports into MongoDB
        load_reports(reports)

    # Define task dependencies
    raw_data = extract()
    processed_reports = transform(raw_data)
    load(processed_reports)
