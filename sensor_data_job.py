import psycopg2
import configparser

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration values
db_config = config['database']
DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']

conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
cursor = conn.cursor()

cursor.execute("""
    SELECT sensor_id, window_start, avg_temperature, min_temperature, max_temperature 
    FROM sensor_temperatures
""")
rows = cursor.fetchall()

aggregated_data = {}
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

    aggregated_data[key]['total_avg_temp'] += avg_temp
    aggregated_data[key]['total_min_temp'] = min(aggregated_data[key]['total_min_temp'], min_temp)
    aggregated_data[key]['total_max_temp'] = max(aggregated_data[key]['total_max_temp'], max_temp)
    aggregated_data[key]['count'] += 1

to_insert = []
for key, value in aggregated_data.items():
    sensor_id, record_date, record_hour = key
    avg_temp = value['total_avg_temp'] / value['count']
    min_temp = value['total_min_temp']
    max_temp = value['total_max_temp']
    to_insert.append((sensor_id, record_date, record_hour, avg_temp, min_temp, max_temp))

insert_query = """
    INSERT INTO sensor_temperatures_hourly (sensor_id, record_date, record_hour, avg_temperature, min_temperature, max_temperature)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (sensor_id, record_date, record_hour) DO UPDATE
    SET avg_temperature = EXCLUDED.avg_temperature,
        min_temperature = EXCLUDED.min_temperature,
        max_temperature = EXCLUDED.max_temperature
"""

cursor.executemany(insert_query, to_insert)

conn.commit()

cursor.close()
conn.close()