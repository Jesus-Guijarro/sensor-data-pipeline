import os
import time
import json
from confluent_kafka import Producer
from .sensor import Sensor
from database.database import get_connection

# Kafka configuration: read bootstrap servers from env or use default
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Topic names for sensor data and logs
SENSOR_TOPIC = 'sensor-data'
LOG_TOPIC = 'log-data'

def delivery_report(err, msg):
    """
    Callback to report the delivery status of Kafka messages.

    :param err: Error information if delivery failed, otherwise None
    :param msg: The message object containing topic and partition info
    """
    if err is not None:
        # Print error details if delivery failed
        print(f"Message delivery failed: {err}")
    else:
        # Confirm successful delivery with topic and partition
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def load_sensor_configurations():
    """
    Load sensor_id and diff_temperature values from the sensors table in PostgreSQL.

    :return: List of tuples (sensor_id, diff_temperature)
    """
    # Establish database connection and cursor
    conn, cursor = get_connection()
    # Fetch sensor configurations ordered by sensor_id
    cursor.execute("SELECT sensor_id, diff_temperature FROM sensors ORDER BY sensor_id;")
    configs = cursor.fetchall()
    # Close DB resources
    cursor.close()
    conn.close()
    return configs


def produce_one_round(producer, sensors):
    """
    Send one batch of sensor messages; returns the number of messages produced.

    Iterates through each Sensor instance, generates data, categorizes into
    normal sensor data or log messages (error/warning), and publishes to Kafka.

    :param producer: Confluent Kafka Producer instance
    :param sensors: List of Sensor objects to query
    :return: int, count of messages produced
    """
    count = 0
    for sensor in sensors:
        # Generate data or anomaly for this sensor
        data = sensor.generate_sensor_data()

        if data["temperature"] is None:
            # Sensor disconnected: prepare ERROR log message
            topic = LOG_TOPIC
            level = "ERROR"
            msg = {
                "sensor_id": data["sensor_id"],
                "level": level,
                "message": f"Sensor {data['sensor_id']} disconnected"
            }
        elif data.get("anomaly") == "Measurement_error":
            # Measurement anomaly: prepare WARNING log with details
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
            # Normal operation: send raw sensor data
            topic = SENSOR_TOPIC
            msg = data

        # Produce message asynchronously with callback
        producer.produce(topic, value=json.dumps(msg), callback=delivery_report)
        # Trigger delivery callbacks
        producer.poll(0)
        count += 1
    return count


def main():
    """
    Main loop: initializes producer, loads sensors, and continuously sends data.

    Runs indefinitely until interrupted, ensuring graceful shutdown on exit.
    """
    # Initialize Kafka producer with bootstrap server config
    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    # Load sensor configurations from database
    sensor_configs = load_sensor_configurations()
    # Instantiate Sensor objects with ID and temperature offset
    sensors = [Sensor(sensor_id=id_, diff_temperature=diff)
               for id_, diff in sensor_configs]

    try:
        # Continuously produce sensor data at 1-second intervals
        while True:
            produce_one_round(producer, sensors)
            time.sleep(1)

    except KeyboardInterrupt:
        # Handle user-initiated shutdown
        print("Shutting down producer...")
    finally:
        # Ensure all queued messages are delivered before exit
        producer.flush()


if __name__ == '__main__':
    main()