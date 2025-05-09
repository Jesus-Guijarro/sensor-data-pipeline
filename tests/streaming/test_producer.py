import json
import pytest
from streaming.producer import (
    load_sensor_configurations,
    delivery_report,
    produce_one_round,
    SENSOR_TOPIC,
    LOG_TOPIC,
)

class DummyCursor:
    def execute(self, q): pass
    def fetchall(self): return [(1, 0.5), (2, -1.2)]
    def close(self): pass

class DummyConn:
    def __init__(self): self.cursor = DummyCursor()
    def __enter__(self): return self.cursor
    def __exit__(self,*a): pass
    def close(self): pass

def test_load_sensor_configurations(monkeypatch):
    # Monkey-patch get_connection() to return our dummy
    import streaming.producer as producer
    monkeypatch.setattr(
        "producer.get_connection",
        lambda: (DummyConn(), DummyConn().cursor)
    )
    configs = load_sensor_configurations()
    assert configs == [(1, 0.5), (2, -1.2)]

def test_delivery_report_success(capfd):
    # Simulate msg.topic()/partition()
    class Msg: 
        def topic(self): return "t"
        def partition(self): return 3

    delivery_report(None, Msg())
    captured = capfd.readouterr().out
    assert "Message delivered to t [3]" in captured

def test_delivery_report_failure(capfd):
    delivery_report(Exception("fail"), None)
    captured = capfd.readouterr().out
    assert "Message delivery failed: fail" in captured

class FakeProducer:
    def __init__(self):
        self.produced = []
        self.polled = 0
    def produce(self, topic, value, callback):
        # record what was called
        self.produced.append((topic, json.loads(value)))
        callback(None, type("M", (), {"topic": lambda:self.produced[-1][0],
                                      "partition": lambda: 0}))
    def poll(self, timeout):
        self.polled += 1
    def flush(self):
        pass

class DummySensor:
    def __init__(self, sensor_id, data):
        self.sensor_id = sensor_id
        self._data = data
    def generate_sensor_data(self):
        return self._data

@pytest.mark.parametrize("data,expected_topic", [
    ({"sensor_id":1,"timestamp":0,"temperature":None,"humidity":None}, LOG_TOPIC),
    ({"sensor_id":2,"timestamp":0,"temperature":10,"humidity":20,
      "anomaly":"Measurement_error"}, LOG_TOPIC),
    ({"sensor_id":3,"timestamp":0,"temperature":15,"humidity":30}, SENSOR_TOPIC),
])
def test_produce_one_round_routes_correctly(data, expected_topic):
    prod = FakeProducer()
    sensor = DummySensor(data["sensor_id"], data)
    count = produce_one_round(prod, [sensor])
    # We should have produced exactly 1 message
    assert count == 1
    topic, payload = prod.produced[0]
    assert topic == expected_topic
    # payload must have sensor_id
    assert payload["sensor_id"] == data["sensor_id"]

