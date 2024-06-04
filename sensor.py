import random
import time
from kafka import KafkaProducer
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'sensor-data'

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to generate sensor data
def generate_sensor_data(sensor_id):
    # Generate normal data
    temperature = round(random.uniform(20, 25), 2)  # Normal temperature between 20 and 25 degrees
    humidity = round(random.uniform(30, 50), 2)     # Normal humidity between 30 and 50%

    return {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()), # Current time as timestamp
        'temperature': temperature,
        'humidity': humidity
    }

# Continuously send data
sensor_count = 50  # Number of sensors
while True:
    for sensor_id in range(sensor_count):
        data = generate_sensor_data(sensor_id)

        # Send the generated data to the Kafka topic
        producer.send(topic, value=data)

    time.sleep(1)  # Send data every second
