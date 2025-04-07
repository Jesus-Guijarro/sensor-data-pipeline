import time
import json
from confluent_kafka import Producer
from sensor import Sensor

def delivery_report(err, msg):
    # Called once for each produced message to indicate delivery result.
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    sensor_topic = 'sensor-data'
    log_topic = 'log-data'

    # Kafka producer configuration using confluent_kafka
    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    number_sensors = 20  # Number of sensors
    sensors = [Sensor(sensor_id=i) for i in range(1, number_sensors + 1)]
    
    # Infinite loop to generate and send sensor data
    while True:
        for sensor in sensors:
            data = sensor.generate_sensor_data()

            # Check if temperature data is valid before sending to Kafka
            if data["temperature"] is None:
                message = {
                    "sensor_id": data["sensor_id"],
                    "level": "ERROR",
                    "message": f"Sensor ID {data['sensor_id']}: disconnected"
                }
                producer.produce(log_topic, value=json.dumps(message), callback=delivery_report)
            elif data.get("anomaly"):
                message = {
                    "sensor_id": data["sensor_id"],
                    "level": "WARNING",
                    "message": f"Sensor ID {data['sensor_id']}: measurement error detected: {data['temperature']}ºC"
                }
                producer.produce(log_topic, value=json.dumps(message), callback=delivery_report)
            else:
                producer.produce(sensor_topic, value=json.dumps(data), callback=delivery_report)

            # Poll to handle delivery reports (non-blocking)
            producer.poll(0)

        time.sleep(1)  # Send data every second
        
        producer.flush()
