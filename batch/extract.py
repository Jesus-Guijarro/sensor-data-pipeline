import pandas as pd
from datetime import datetime, timedelta
from database.database import get_connection

def extract_sensor_readings(date_str: str) -> pd.DataFrame:
    
    conn, cursor = get_connection()

    start = datetime.fromisoformat(date_str)
    end   = start + timedelta(days=1)

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
    WHERE sr.window_start = %s
      AND sr.window_start <=  %s
    ORDER BY sr.sensor_id, sr.window_start
    """

    cursor.execute(query, (start, end))

    rows = cursor.fetchall()

    cols = [desc[0] for desc in cursor.description]

    conn.close()

    df = pd.DataFrame(rows, columns=cols)
    df['date_str'] = df['window_start'].dt.strftime('%Y-%m-%d')
    
    return df