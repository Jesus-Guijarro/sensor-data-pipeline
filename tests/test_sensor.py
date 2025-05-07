import pytest
import random
import time
from sensor import Sensor

def test_generate_normal_data_returns_int_and_valid_humidity():
    sensor = Sensor(sensor_id=1)
    temp, hum = sensor.generate_normal_data()
    # Returns two integer values
    assert isinstance(temp, int)
    assert isinstance(hum, int)
    # Humidity is always between 0 and 100
    assert 0 <= hum <= 100

def test_generate_sensor_data_no_anomaly(monkeypatch):
    sensor = Sensor(sensor_id=42)
    # Force no anomaly ever occurs
    sensor.error_probability = 0.0
    # Make generate_normal_data always return the same values
    monkeypatch.setattr(sensor, "generate_normal_data", lambda: (23, 60))

    d = sensor.generate_sensor_data()
    assert d["sensor_id"] == 42
    assert isinstance(d["timestamp"], int)
    assert d["temperature"] == 23
    assert d["humidity"] == 60
    # No 'anomaly' key should be present
    assert "anomaly" not in d

def test_generate_sensor_data_measurement_error(monkeypatch):
    sensor = Sensor(sensor_id=7)
    # Force anomaly always occurs
    sensor.error_probability = 1.0

    # 1) Make random.choice pick 'Measurement_error'
    monkeypatch.setattr(
        random, "choice",
        lambda seq: "Measurement_error" if seq == ['Measurement_error', 'Disconnect'] else seq[0]
    )
    # 2) Stub generate_normal_data to fixed values
    monkeypatch.setattr(sensor, "generate_normal_data", lambda: (20, 50))
    # 3) Stub random.uniform to a fixed offset
    monkeypatch.setattr(random, "uniform", lambda a, b: 7)

    d = sensor.generate_sensor_data()
    assert d["sensor_id"] == 7
    # Temperature should be 20 + 7
    assert d["temperature"] == 27
    # Humidity remains the same
    assert d["humidity"] == 50
    assert d["anomaly"] == "Measurement_error"

def test_generate_sensor_data_disconnect(monkeypatch):
    sensor = Sensor(sensor_id=99)
    # Force anomaly always occurs
    sensor.error_probability = 1.0

    # Make random.choice pick 'Disconnect'
    monkeypatch.setattr(random, "choice", lambda seq: "Disconnect")

    d = sensor.generate_sensor_data()
    assert d["sensor_id"] == 99
    # On disconnect both readings are None
    assert d["temperature"] is None
    assert d["humidity"] is None
    assert d["anomaly"] == "Disconnect"
