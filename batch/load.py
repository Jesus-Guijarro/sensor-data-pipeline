import os
from datetime import datetime
from pymongo import MongoClient

def load_reports(reports: list):
    uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    client = MongoClient(uri)
    db = client.sensor_data
    col = db.reports

    for rpt in reports:
        sensor_id = int(rpt["sensor_id"])

        date_str = rpt["date"]
        date_dt  = datetime.strptime(date_str, "%Y-%m-%d")

        clean_rpt = {
            "sensor_id": sensor_id,
            "city":      rpt["city"],
            "station":   rpt["station"],
            "date":      date_dt,
            "data":      rpt["data"],
        }

        filtro = {
            "sensor_id": sensor_id,
            "date":      date_dt
        }
        col.replace_one(filtro, clean_rpt, upsert=True)

    client.close()