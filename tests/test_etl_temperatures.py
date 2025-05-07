def test_transform_handles_outliers():
    from etl.sensor_temperatures import transform
    data = [{"sensor_id": 1, "temp": 999}]   # outlier
    clean = transform(data)
    assert clean[0]["temp"] is None
