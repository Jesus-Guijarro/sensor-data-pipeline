import pandas as pd

def transform_sensor_readings(df: pd.DataFrame) -> list:
    """
    Transform raw sensor readings into a list of report dictionaries.

    Processes a DataFrame of sensor data by grouping on sensor, location, and date,
    computing summary statistics for temperature and humidity, and aggregating hourly readings.

    Parameters:
        df (pd.DataFrame): DataFrame with columns:
            - sensor_id (int): Unique identifier of the sensor.
            - city (str): City where the sensor is located.
            - station (str): Station identifier.
            - window_start (datetime): Timestamp for the start of the measurement window.
            - temperature (float): Recorded temperature value.
            - humidity (float): Recorded humidity value.

    Returns:
        list: A list of dictionaries, each with keys:
            - sensor_id (int)
            - city (str)
            - station (str)
            - date (str): Date in 'YYYY-MM-DD' format.
            - data (dict): Contains:
                - min_temperature (int)
                - max_temperature (int)
                - avg_temperature (int)
                - min_humidity (int)
                - max_humidity (int)
                - avg_humidity (int)
                - hourly_readings (list): List of dicts for each hour with:
                    * hour (int)
                    * temperature (int)
                    * humidity (int)
    """
    # Return empty list if input DataFrame has no records
    if df.empty:
        return []

    # Ensure 'window_start' is in datetime format
    df['window_start'] = pd.to_datetime(df['window_start'])

    # Extract date string for grouping
    df['date'] = df['window_start'].dt.date.astype(str)

    # Extract hour of day for hourly aggregation
    df['hour'] = df['window_start'].dt.hour

    reports = []
    group_cols = ['sensor_id', 'city', 'station', 'date']

    # Iterate over each group to compute statistics and assemble report
    for (sensor_id, city, station, date), grp in df.groupby(group_cols):

        # Temperature metrics
        min_t = int(grp.temperature.min())
        max_t = int(grp.temperature.max())
        avg_t = round(grp.temperature.mean())

        # Humidity metrics
        min_h = int(grp.humidity.min())
        max_h = int(grp.humidity.max())
        avg_h = round(grp.humidity.mean())

        # Compile hourly averages
        hourly = []
        for h in range(24):
            sub = grp[grp.hour == h]
            if not sub.empty:
                t = round(sub.temperature.mean())
                hmd = round(sub.humidity.mean())
            else:
                t = None
                hmd = None
            hourly.append({
                "hour": h,
                "temperature": t,
                "humidity": hmd
            })

        # Append structured report dict
        reports.append({
            'sensor_id': sensor_id,
            'city': city,
            'station': station,
            'date': date,
            'data': {
                'min_temperature': min_t,
                'max_temperature': max_t,
                'avg_temperature': avg_t,
                'min_humidity': min_h,
                'max_humidity': max_h,
                'avg_humidity': avg_h,
                'hourly_readings': hourly
            }
        })

    return reports