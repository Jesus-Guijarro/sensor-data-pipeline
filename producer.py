import time
from kafka import KafkaProducer
import json
import logging
from sensor import Sensor
    
if __name__ == "__main__":
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    sensor_topic = 'sensor-data'
    log_topic = 'log-data'

    # Kafka producer configuration
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    number_sensors = 20  # Number of sensors
    sensors = [Sensor(sensor_id=i) for i in range(1,(number_sensors+1))]
    
    # Infinite loop to generate and send sensor data
    while True:
        for sensor_id in sensors:
            data = sensor_id.generate_sensor_data()

            # Check if temperature data is valid before sending to Kafka
            if data["temperature"] is None:
                producer.send(log_topic, value={"sensor_id": data['sensor_id'], "level": "ERROR", "message": "Disconnected"})
            elif (data["temperature"] <= -10 or data["temperature"] >= 50):
                producer.send(log_topic, value={"sensor_id": data['sensor_id'], "level": "WARNING", "message": f'Invalid temperature: {data["temperature"]}'})
            else:
                producer.send(sensor_topic, value=data)

        time.sleep(1)  # Send data every second
