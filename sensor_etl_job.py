import os
import json
from datetime import datetime

import database.database as db

# Establish database connection
conn, cursor = db.get_connection()

# Retrieve data from sensor_temperatures table
cursor.execute("""
    SELECT sensor_id, window_start, avg_temperature, min_temperature, max_temperature 
    FROM sensor_temperatures
""")
rows = cursor.fetchall()

# Initialize dictionary to aggregate data
aggregated_data = {}

# Iterate over fetched rows and aggregate data
for row in rows:
    sensor_id, window_start, avg_temp, min_temp, max_temp = row
    record_date = window_start.date()
    record_hour = window_start.replace(minute=0, second=0, microsecond=0).time()

    key = (sensor_id, record_date, record_hour)
    if key not in aggregated_data:
        aggregated_data[key] = {
            'total_avg_temp': 0,
            'total_min_temp': float('inf'),
            'total_max_temp': float('-inf'),
            'count': 0
        }
    # Aggregate values
    aggregated_data[key]['total_avg_temp'] += avg_temp
    aggregated_data[key]['total_min_temp'] = min(aggregated_data[key]['total_min_temp'], min_temp)
    aggregated_data[key]['total_max_temp'] = max(aggregated_data[key]['total_max_temp'], max_temp)
    aggregated_data[key]['count'] += 1

# Prepare data for insertion into sensor_temperatures_hourly table
to_insert = []
for key, value in aggregated_data.items():
    sensor_id, record_date, record_hour = key
    avg_temp = value['total_avg_temp'] / value['count']
    min_temp = value['total_min_temp']
    max_temp = value['total_max_temp']
    to_insert.append((sensor_id, record_date, record_hour, avg_temp, min_temp, max_temp))

# SQL query for insertion
insert_query = """
    INSERT INTO sensor_temperatures_hourly (sensor_id, record_date, record_hour, avg_temperature, min_temperature, max_temperature)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (sensor_id, record_date, record_hour) DO UPDATE
    SET avg_temperature = EXCLUDED.avg_temperature,
        min_temperature = EXCLUDED.min_temperature,
        max_temperature = EXCLUDED.max_temperature
"""
# Execute insertion query
cursor.executemany(insert_query, to_insert)

# Commit changes to the database
conn.commit()

# Generate JSON data from queried results
query = '''
SELECT 
    record_date,
    AVG(avg_temperature) as tmed,
    MIN(min_temperature) as tmin,
    MAX(max_temperature) as tmax,
    (SELECT record_hour FROM sensor_temperatures_hourly WHERE record_date = a.record_date AND min_temperature = MIN(a.min_temperature) LIMIT 1) as horatmin,
    (SELECT record_hour FROM sensor_temperatures_hourly WHERE record_date = a.record_date AND max_temperature = MAX(a.max_temperature) LIMIT 1) as horatmax
FROM sensor_temperatures_hourly a
GROUP BY record_date
'''

cursor.execute(query)
rows = cursor.fetchall()

# Constant values for JSON data
indicativo = "8025"             #station code
nombre = "ALACANT/ALICANTE"     #station name
provincia = "ALICANTE"          #province name

data_list = []
for row in rows:
    # Format each row into a JSON record
    record = {
        "fecha": row[0].strftime('%Y-%m-%d'),
        "indicativo": indicativo,
        "nombre": nombre,
        "provincia": provincia,
        "tmed": f"{row[1]:.1f}",
        "tmin": f"{row[2]:.1f}",
        "tmax": f"{row[3]:.1f}",
        "horatmin": row[4].strftime('%H:%M'),
        "horatmax": row[5].strftime('%H:%M')
    }
    data_list.append(record)

# Get today's date
today = datetime.now().strftime('%Y-%m-%d')

# Ensure 'data' directory exists
os.makedirs('data', exist_ok=True)

# Write JSON data to a file
json_file_path = f"data/{today}_{indicativo}.json"
with open(json_file_path, 'w', encoding='utf-8') as f:
    json.dump(data_list, f, ensure_ascii=False, indent=4)

# Close cursor and database connection
cursor.close()
conn.close()