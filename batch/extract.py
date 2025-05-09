import pandas as pd
from database.database import get_connection

def extract_sensor_readings(start_ts: str, end_ts: str) -> pd.DataFrame:
    conn, cursor = get_connection()

    query = """
    SELECT 
      sr.sensor_id,
      s.city,
      s.station,
      sr.window_start,
      sr.window_end,
      sr.temperature,
      sr.humidity
    FROM sensor_readings sr
    JOIN sensors s ON s.sensor_id = sr.sensor_id
    WHERE sr.window_start >= %s
      AND sr.window_start <  %s
    ORDER BY sr.sensor_id, sr.window_start
    """

    cursor.execute(query, (start_ts, end_ts))

    rows = cursor.fetchall()

    cols = [desc[0] for desc in cursor.description]

    conn.close()
    
    return pd.DataFrame(rows, columns=cols)