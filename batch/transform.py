import pandas as pd

def transform_sensor_readings(df: pd.DataFrame) -> list:
    if df.empty:
        return []

    df['window_start'] = pd.to_datetime(df['window_start'])
    df['date']  = df['window_start'].dt.date.astype(str)
    df['hour']  = df['window_start'].dt.hour

    reports = []
    group_cols = ['sensor_id', 'city', 'station', 'date']
    for (sensor_id, city, station, date), grp in df.groupby(group_cols):
        min_t  = int(grp.temperature.min())
        max_t  = int(grp.temperature.max())
        avg_t  = round(grp.temperature.mean())
        min_h  = int(grp.humidity.min())
        max_h  = int(grp.humidity.max())
        avg_h  = round(grp.humidity.mean())

        hourly = []
        for h in range(24):
            sub = grp[grp.hour == h]
            if not sub.empty:
                t = round(sub.temperature.mean(), 2)
                hmd = round(sub.humidity.mean(), 2)
            else:
                t = None
                hmd = None
            hourly.append({
                "hour": h,
                "temperature": t,
                "humidity": hmd
            })

        reports.append({
            "sensor_id": sensor_id,
            "city": city,
            "station": station,
            "date": date,
            "data": {
                "min_temperature": min_t,
                "max_temperature": max_t,
                "avg_temperature": avg_t,
                "min_humidity": min_h,
                "max_humidity": max_h,
                "avg_humidity": avg_h,
                "hourly_readings": hourly
            }
        })

    return reports