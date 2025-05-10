import os
from datetime import datetime
from pymongo import MongoClient

def load_reports(reports: list) -> None:
    """
    Upsert a list of sensor report dictionaries into MongoDB.

    Connects to the MongoDB instance specified by the MONGO_URI environment variable
    (defaults to localhost), and ensures each report is inserted or replaced
    based on a unique key of sensor_id and date.

    Parameters:
        reports (list): List of dictionaries containing report data with keys:
            - sensor_id (int): Identifier of the sensor.
            - city (str): City where the sensor is located.
            - station (str): Station identifier string.
            - date (str): ISO formatted date string 'YYYY-MM-DD'.
            - data (dict): Aggregated sensor readings and metadata.

    Returns:
        None
    """
    # Determine MongoDB URI from environment or use default
    uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    client = MongoClient(uri)
    db = client.sensor_data_batch
    col = db.reports

    # Iterate through each report and upsert into the collection
    for rpt in reports:
        # Convert sensor_id to integer and parse date string to datetime
        sensor_id = int(rpt['sensor_id'])
        date_dt = datetime.strptime(rpt['date'], '%Y-%m-%d')

        # Construct the document to store in MongoDB
        clean_rpt = {
            'sensor_id': sensor_id,
            'city':       rpt['city'],
            'station':    rpt['station'],
            'date':       date_dt,
            'data':       rpt['data'],
        }

        # Define unique filter criteria for upsert operation
        filter_criteria = {
            'sensor_id': sensor_id,
            'date':      date_dt,
        }

        # Replace existing document or insert new one
        col.replace_one(filter_criteria, clean_rpt, upsert=True)

    client.close()
