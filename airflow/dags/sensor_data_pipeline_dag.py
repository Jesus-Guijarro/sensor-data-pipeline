"""
DAG definition for the sensor batch processing pipeline.

Defines an Airflow DAG that orchestrates daily ETL tasks:
- extract sensor readings from the database,
- transform raw data into structured reports,
- load reports into MongoDB.
"""
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
import pendulum

# Import ETL functions from batch modules
from batch.extract import extract_sensor_readings
from batch.transform import transform_sensor_readings
from batch.load import load_reports

# Default arguments applied to all DAG tasks
default_args = {
    'owner': 'airflow',             # Owner of the DAG
    'depends_on_past': False,       # Do not wait for previous runs
    'retries': 1,                   # Number of retry attempts on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG with a daily schedule at 00:01 UTC, no backfill
with DAG(
    dag_id='sensor_batch_pipeline',
    default_args=default_args,
    description='Sensors batch pipeline',
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Madrid"),
    schedule='1 0 * * *',
    catchup=False,                             # Skip backfill of past intervals
    tags=['etl', 'sensors'],                   # Tags for categorization
) as dag:

    @task()
    def extract(date_str: str):
        """
        Extract task: retrieve sensor readings for the given date.

        Parameters:
            date_str (str): ISO formatted date string marking the end of the data interval.

        Returns:
            str: JSON string of the raw DataFrame (ISO date format, split orientation).
        """
        df = extract_sensor_readings(date_str)
        return df.to_json(date_format='iso', orient='split')

    @task()
    def transform(df_json: str):
        """
        Transform task: convert raw JSON data into structured report dictionaries.

        Parameters:
            df_json (str): JSON string of the raw DataFrame (split orientation).

        Returns:
            list: List of report dictionaries generated by transform_sensor_readings.
        """
        import pandas as pd
        df = pd.read_json(df_json, orient='split')
        reports = transform_sensor_readings(df)
        return reports

    @task()
    def load(reports: list):
        """
        Load task: upsert report documents into MongoDB.

        Parameters:
            reports (list): List of report dictionaries to load into the database.

        Returns:
            None
        """
        # Execute the upsert operation for all reports
        load_reports(reports)

    # Define task dependencies: extract >> transform >> load
    raw_data = extract(date_str='{{ macros.ds_add(ds, -1) }}')
    processed_reports = transform(raw_data)
    load(processed_reports)
