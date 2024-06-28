import random
import time
from kafka import KafkaProducer
import json
import logging

# Function to generate sensor data
def generate_sensor_data(sensor_id):

    anomaly_probability = 0.01  # 1% chance of an anomaly

    random_value = random.random()

    # Check if an anomaly should be generated
    if random_value < anomaly_probability:
        # Generate anomalous data
        anomaly_type = random.choice(['extreme', 'disconnect'])
        if anomaly_type == 'extreme':
            temperature = round(random.choice([random.uniform(-20, -10), random.uniform(50, 60)]), 2)
            logging.warning(f'Sensor ID {sensor_id}: anomalous extreme temperature detected: {temperature}°C')
        # Disconnection
        elif anomaly_type == 'disconnect':
            temperature = 0
            logging.error(f'Sensor ID {sensor_id}: disconnected')
    else:
        temperature = round(random.uniform(15, 30), 2)  # Normal temperature between 15 and 30 degrees

    return {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()), # Current time as timestamp
        'temperature': temperature
    }
    
if __name__ == "__main__":
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'sensor-data'

    # Kafka producer configuration
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    logging.basicConfig(filename='logs/sensor_logs.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    sensor_count = 100  # Number of sensors
    while True:
        for sensor_id in range(sensor_count):
            data = generate_sensor_data(sensor_id)

            # Send the generated data to the Kafka topic
            if data['temperature'] != 0 or (data['temperature'] > -10 and data['temperature'] < 50):
                producer.send(topic, value=data)

        time.sleep(1)  # Send data every second
