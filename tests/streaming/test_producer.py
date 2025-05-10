import json
import streaming.producer as producer 

def test_load_sensor_configurations(monkeypatch):
    """Test that load_sensor_configurations retrieves sensor configs from the DB and closes the connection."""
    # Set up a dummy database connection and cursor with predefined results
    dummy_result = [(1, 2.5), (2, 1.0)]
    class DummyCursor:
        def __init__(self):
            self.closed = False
        def execute(self, query):
            # Simulate execution of query (no operation needed for test)
            self.last_query = query
        def fetchall(self):
            # Return a predefined list of sensor configurations
            return dummy_result
        def close(self):
            # Mark cursor as closed
            self.closed = True
    class DummyConn:
        def __init__(self):
            self.closed = False
        def close(self):
            # Mark connection as closed
            self.closed = True

    dummy_conn = DummyConn()
    dummy_cursor = DummyCursor()
    def dummy_get_connection():
        # Return our dummy connection and cursor
        return dummy_conn, dummy_cursor

    # Monkeypatch the database get_connection function used in producer.py
    monkeypatch.setattr(producer, 'get_connection', dummy_get_connection)

    # Call the function under test
    configs = producer.load_sensor_configurations()

    # It should return the data from fetchall()
    assert configs == dummy_result, "Should return list of sensor configurations from the database"
    # The connection and cursor should be closed after function execution
    assert dummy_conn.closed and dummy_cursor.closed, "Database connection and cursor should be closed"

def test_produce_one_round_normal(monkeypatch):
    """Test produce_one_round with a normal sensor reading (no anomaly)."""
    # Create a dummy sensor that returns a normal data dictionary (temperature is not None, no anomaly key)
    dummy_data = {"sensor_id": 1, "temperature": 22, "humidity": 45}
    class DummySensor:
        def generate_sensor_data(self):
            return dummy_data
    sensors = [DummySensor()]

    # Create a dummy Kafka Producer that tracks produce calls
    produced_messages = []
    class DummyProducer:
        def produce(self, topic, value, callback=None):
            # Record the topic and JSON-decoded message
            produced_messages.append((topic, json.loads(value)))
            # (We do not need to call the callback in this test)
        def poll(self, timeout):
            # No-op for polling in tests
            pass

    producer_instance = DummyProducer()
    # Call produce_one_round with one sensor that has normal data
    count = producer.produce_one_round(producer_instance, sensors)

    # It should produce exactly one message for the one sensor
    assert count == 1, "Should produce one message for one sensor"
    # Inspect the produced message
    topic, message = produced_messages[0]
    # The message should be sent to the sensor data topic for normal readings
    assert topic == producer.SENSOR_TOPIC, "Normal readings should be sent to SENSOR_TOPIC"
    # The message content should exactly match the sensor data dictionary
    assert message == dummy_data, "Produced message should match the sensor's data output"

def test_produce_one_round_measurement_error(monkeypatch):
    """Test produce_one_round with a sensor measurement error anomaly (anomaly flag set to Measurement_error)."""
    # Dummy sensor returns data indicating a measurement error (temperature present but flagged as anomaly)
    sensor_id = 5
    dummy_data = {
        "sensor_id": sensor_id,
        "temperature": 30,
        "humidity": 70,
        "anomaly": "Measurement_error"
    }
    class DummySensor:
        def generate_sensor_data(self):
            return dummy_data
    sensors = [DummySensor()]

    # Dummy producer to capture produce calls
    produced_messages = []
    class DummyProducer:
        def produce(self, topic, value, callback=None):
            produced_messages.append((topic, json.loads(value)))
        def poll(self, timeout):
            pass

    producer_instance = DummyProducer()
    count = producer.produce_one_round(producer_instance, sensors)

    # One message should be produced for the sensor
    assert count == 1
    topic, message = produced_messages[0]
    # In case of measurement error, the message should go to the log topic with WARNING level
    assert topic == producer.LOG_TOPIC, "Measurement errors should be sent to LOG_TOPIC"
    assert message.get("level") == "WARNING", "Log message level should be WARNING for measurement errors"
    assert message.get("sensor_id") == sensor_id, "Log message should contain the correct sensor_id"
    # The log message text should mention a measurement error for that sensor
    assert "Measurement error on sensor" in message.get("message", ""), "Log message should describe the measurement error"

def test_produce_one_round_disconnect(monkeypatch):
    """Test produce_one_round with a sensor disconnect anomaly (temperature is None)."""
    # Dummy sensor returns data indicating a disconnect (temperature is None, simulating a sensor failure)
    sensor_id = 3
    dummy_data = {
        "sensor_id": sensor_id,
        "temperature": None,
        "humidity": None
        # Note: 'anomaly': 'Disconnect' could be included by Sensor, but produce_one_round uses temperature None to detect disconnect
    }
    class DummySensor:
        def generate_sensor_data(self):
            return dummy_data
    sensors = [DummySensor()]

    produced_messages = []
    class DummyProducer:
        def produce(self, topic, value, callback=None):
            produced_messages.append((topic, json.loads(value)))
        def poll(self, timeout):
            pass

    producer_instance = DummyProducer()
    count = producer.produce_one_round(producer_instance, sensors)

    # Expect one message produced for the sensor
    assert count == 1
    topic, message = produced_messages[0]
    # Disconnect events should be sent to the log topic with ERROR level
    assert topic == producer.LOG_TOPIC, "Disconnect events should be sent to LOG_TOPIC"
    assert message.get("level") == "ERROR", "Log message level should be ERROR for disconnects"
    assert message.get("sensor_id") == sensor_id, "Log message should contain the correct sensor_id"
    # The log message text should indicate that the sensor was disconnected
    assert "disconnected" in message.get("message", ""), "Log message should indicate sensor disconnect"
