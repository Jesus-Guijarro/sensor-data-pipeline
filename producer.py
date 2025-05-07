import os
import time
import json
from confluent_kafka import Producer
from sensor import Sensor
from database.database import get_connection

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SENSOR_TOPIC = 'sensor-data'
LOG_TOPIC = 'log-data'

def delivery_report(err, msg):
    """
    Callback to report the delivery status of Kafka messages.

    :param err: Error information if delivery failed, otherwise None.
    :param msg: The message object containing topic and partition info.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def load_sensor_configurations():
    """
    Load sensor_id and diff_temperature values from the sensors table in PostgreSQL.

    :return: List of tuples (sensor_id, diff_temperature)
    """
    conn, cursor = get_connection()
    cursor.execute("SELECT sensor_id, diff_temperature FROM sensors ORDER BY sensor_id;")
    configs = cursor.fetchall()
    cursor.close()
    conn.close()
    return configs

def produce_one_round(producer, sensors):
    """
    Send one batch of sensor messages, returning the number of messages produced.
    """
    count = 0
    for sensor in sensors:
        data = sensor.generate_sensor_data()

        if data["temperature"] is None:
            topic = LOG_TOPIC
            level = "ERROR"
            msg = {
                "sensor_id": data["sensor_id"],
                "level": level,
                "message": f"Sensor {data['sensor_id']} disconnected"
            }
        elif data.get("anomaly") == "Measurement_error":
            topic = LOG_TOPIC
            level = "WARNING"
            msg = {
                "sensor_id": data["sensor_id"],
                "level": level,
                "message": (
                    f"Measurement error on sensor {data['sensor_id']}: "
                    f"{data['temperature']}°C, humidity {data['humidity']}%"
                )
            }
        else:
            topic = SENSOR_TOPIC
            msg = data

        producer.produce(topic, value=json.dumps(msg), callback=delivery_report)
        producer.poll(0)
        count += 1
    return count

def main():
    # Create Kafka producer instance
    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    # Load sensor configurations from the database
    sensor_configs = load_sensor_configurations()
    sensors = [Sensor(sensor_id=id_, diff_temperature=diff)
               for id_, diff in sensor_configs]

    try:
        while True:
            produce_one_round(producer, sensors)
            time.sleep(1)

    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        # Ensure all messages are sent before exiting
        producer.flush()

if __name__ == '__main__':
    main()
