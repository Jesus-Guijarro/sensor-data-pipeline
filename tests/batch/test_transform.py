import pandas as pd
import batch.transform as transform

def test_transform_sensor_readings_empty():
    """Test that transform_sensor_readings returns an empty list when given an empty DataFrame."""
    empty_df = pd.DataFrame([])
    result = transform.transform_sensor_readings(empty_df)
    # An empty input should produce an empty list output
    assert result == [], "Expected an empty list for empty input DataFrame"

def test_transform_sensor_readings_aggregation():
    """Test transform_sensor_readings aggregates data correctly for a given day's sensor readings."""
    # Create a small DataFrame with multiple readings for one sensor on the same date
    data = [
        {"sensor_id": 1, "city": "CityA", "station": "StationX", "window_start": "2025-01-01 00:00:00", "temperature": 10.0, "humidity": 50.0},
        {"sensor_id": 1, "city": "CityA", "station": "StationX", "window_start": "2025-01-01 01:00:00", "temperature": 20.0, "humidity": 60.0},
        {"sensor_id": 1, "city": "CityA", "station": "StationX", "window_start": "2025-01-01 01:30:00", "temperature": 22.0, "humidity": 58.0},
        {"sensor_id": 1, "city": "CityA", "station": "StationX", "window_start": "2025-01-01 02:00:00", "temperature": 15.0, "humidity": 55.0}
    ]
    df = pd.DataFrame(data)
    result = transform.transform_sensor_readings(df)
    # The result should be a list with one report dictionary (since all entries are same sensor and date)
    assert isinstance(result, list) and len(result) == 1, "Output should be a list with one report for one sensor/day"
    report = result[0]
    # Report should contain expected keys
    assert set(report.keys()) == {"sensor_id", "city", "station", "date", "data"}, "Report keys are incorrect"
    # The sensor_id, city, station, and date should match the input data
    assert report["sensor_id"] == 1
    assert report["city"] == "CityA"
    assert report["station"] == "StationX"
    assert report["date"] == "2025-01-01", "Date should be formatted as YYYY-MM-DD"
    # Now verify the aggregated data values
    data_dict = report["data"]
    # Expected summary statistics for temperature and humidity
    assert data_dict["min_temperature"] == 10, "Min temperature calculation is incorrect"
    assert data_dict["max_temperature"] == 22, "Max temperature calculation is incorrect"
    assert data_dict["avg_temperature"] == 17, "Avg temperature should be rounded to nearest int"
    assert data_dict["min_humidity"] == 50, "Min humidity calculation is incorrect"
    assert data_dict["max_humidity"] == 60, "Max humidity calculation is incorrect"
    assert data_dict["avg_humidity"] == 56, "Avg humidity should be rounded to nearest int"
    # Verify hourly readings list
    hourly_readings = data_dict.get("hourly_readings", [])
    # It should contain entries for hours 0, 1, and 2
    hours = {entry['hour'] for entry in hourly_readings}
    assert hours == {0, 1, 2}, "Hourly readings should have entries for hours 0, 1, and 2"
    # Build a dictionary for quick lookup by hour
    hourly_by_hour = {entry['hour']: entry for entry in hourly_readings}
    # Check each hour's averaged values
    assert hourly_by_hour[0]['temperature'] == 10 and hourly_by_hour[0]['humidity'] == 50, "Hour 0 averages incorrect"
    assert hourly_by_hour[1]['temperature'] == 21 and hourly_by_hour[1]['humidity'] == 59, "Hour 1 averages incorrect"
    assert hourly_by_hour[2]['temperature'] == 15 and hourly_by_hour[2]['humidity'] == 55, "Hour 2 averages incorrect"
