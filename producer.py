import time
from kafka import KafkaProducer
import json
import logging
from sensor import Sensor
    
if __name__ == "__main__":
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'sensor-data'

    # Kafka producer configuration
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    logging.basicConfig(filename='logs/sensor_logs.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    number_sensors = 10  # Number of sensors
    sensors = [Sensor(sensor_id=i) for i in range(1,(number_sensors+1))]
    
    while True:
        for sensor_id in sensors:
            data = sensor_id.generate_sensor_data()

            # Send the generated data to the Kafka topic
            if data['temperature'] != 0 and (data['temperature'] > -10 and data['temperature'] < 50):
                producer.send(topic, value=data)

        time.sleep(1)  # Send data every second
