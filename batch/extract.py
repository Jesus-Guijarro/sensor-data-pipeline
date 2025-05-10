import pandas as pd
from datetime import datetime, timedelta
from database.database import get_connection

def extract_sensor_readings(date_str: str) -> pd.DataFrame:
    """
    Retrieve sensor readings for a 24-hour period starting at the given ISO date.

    Connects to the database, executes a parameterized query to fetch readings
    within the specified time window, and returns a DataFrame that includes
    an additional 'date_str' column formatted as 'YYYY-MM-DD'.

    Parameters:
        date_str (str): ISO 8601 formatted date string indicating the start of the window.

    Returns:
        pd.DataFrame: DataFrame with columns:
            - sensor_id (int): Unique identifier of the sensor.
            - city (str): City where the sensor is located.
            - station (str): Station identifier.
            - window_start (datetime): Timestamp for the start of the measurement window.
            - temperature (float): Recorded temperature value.
            - humidity (float): Recorded humidity value.
    """
    # Establish database connection and cursor
    conn, cursor = get_connection()

    # Parse input date and define window bounds
    start = datetime.fromisoformat(date_str)
    end = start + timedelta(days=1)

    # Define SQL query to retrieve sensor readings and associated metadata
    query = """
    SELECT
        sr.sensor_id,
        s.city,
        s.station,
        sr.window_start,
        sr.temperature,
        sr.humidity
    FROM sensor_readings sr
    JOIN sensors s ON s.sensor_id = sr.sensor_id
    WHERE sr.window_start >= %s
      AND sr.window_start < %s
    ORDER BY sr.sensor_id, sr.window_start
    """

    cursor.execute(query, (start, end))

    # Fetch query results and extract column names
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]

    conn.close()

    # Construct DataFrameg
    df = pd.DataFrame(rows, columns=cols)

    return df