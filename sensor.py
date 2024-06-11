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

    anomaly_probability = 0.01  # 1% chance of an anomaly

    random_value = random.random()
    # Check if an anomaly should be generated
    if random_value < anomaly_probability:
        # Generate anomalous data
        anomaly_type = random.choice(['extreme', 'null'])
        if anomaly_type == 'extreme':
            temperature = round(random.choice([random.uniform(-20, 5), random.uniform(40, 50)]), 2)
            humidity = round(random.choice([random.uniform(0, 20), random.uniform(60, 100)]), 2)
        elif anomaly_type == 'null':
            temperature = None  # Null temperature
            humidity = None     # Null humidity
    else:
        # Generate normal data
        temperature = round(random.uniform(15, 30), 2)  # Normal temperature between 15 and 30 degrees
        humidity = round(random.uniform(30, 50), 2)     # Normal humidity between 30 and 50%

    return {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()), # Current time as timestamp
        'temperature': temperature,
        'humidity': humidity
    }

# Continuously send data
sensor_count = 100  # Number of sensors
while True:
    for sensor_id in range(sensor_count):
        data = generate_sensor_data(sensor_id)

        # Send the generated data to the Kafka topic
        producer.send(topic, value=data)

    time.sleep(1)  # Send data every second
