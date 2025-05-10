import streaming.sensor as sensor

def test_generate_normal_data_output_range():
    """Temperature/humidity must be ints, humidity clamped [0,100]."""
    s = sensor.Sensor("s1", diff_temperature=0.0)
    temp, hum = s.generate_normal_data()
    assert isinstance(temp, int) and isinstance(hum, int)
    assert 0 <= hum <= 100

def test_generate_normal_data_humidity_clamping(monkeypatch):
    """Force extreme noise so humidity clamps to 0 or 100."""
    s = sensor.Sensor("s1", 0.0)
    # Upper clamp
    monkeypatch.setattr(sensor.random, 'uniform', lambda a, b: 100 if (a, b)==(-3,3) else 0)
    _, hum_high = s.generate_normal_data()
    assert hum_high == 100
    # Lower clamp
    monkeypatch.setattr(sensor.random, 'uniform', lambda a, b: -100 if (a, b)==(-3,3) else 0)
    _, hum_low = s.generate_normal_data()
    assert hum_low == 0

def test_generate_sensor_data_normal(monkeypatch):
    """No anomaly when error_probability=0."""
    s = sensor.Sensor("s1", 0.0)
    s.error_probability = 0.0
    monkeypatch.setattr(sensor.time, 'time', lambda: 1234)
    data = s.generate_sensor_data()
    assert 'anomaly' not in data
    assert data['temperature'] is not None and data['humidity'] is not None
    assert data['sensor_id'] == "s1"
    assert data['timestamp'] == 1234

def test_generate_sensor_data_measurement_error(monkeypatch):
    """When anomaly is Measurement_error, offset is applied."""
    s = sensor.Sensor("s1", 0.0)
    s.error_probability = 1.0
    monkeypatch.setattr(sensor.random, 'choice', lambda seq: 'Measurement_error')
    monkeypatch.setattr(sensor.random, 'uniform', lambda a, b: 5.0)
    s.generate_normal_data = lambda: (30, 50)
    monkeypatch.setattr(sensor.time, 'time', lambda: 1234)
    data = s.generate_sensor_data()
    assert data['anomaly'] == 'Measurement_error'
    assert data['temperature'] == 35
    assert data['humidity'] == 50
    assert data['timestamp'] == 1234

def test_generate_sensor_data_disconnect(monkeypatch):
    """When anomaly is Disconnect, readings are None."""
    s = sensor.Sensor("s2", 0.0)
    s.error_probability = 1.0
    monkeypatch.setattr(sensor.random, 'choice', lambda seq: 'Disconnect')
    monkeypatch.setattr(sensor.time, 'time', lambda: 1234)
    data = s.generate_sensor_data()
    assert data['anomaly'] == 'Disconnect'
    assert data['temperature'] is None and data['humidity'] is None
    assert data['timestamp'] == 1234
