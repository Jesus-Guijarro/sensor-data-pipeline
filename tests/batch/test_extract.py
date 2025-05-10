import pandas as pd
from datetime import datetime, timedelta
import batch.extract as extract

def test_extract_sensor_readings(monkeypatch):
    """Test that extract_sensor_readings executes the correct SQL and returns a DataFrame with expected columns and data."""
    # Prepare dummy database connection and cursor
    class DummyCursor:
        def __init__(self):
            self.closed = False
            self.query = None
            self.params = None
            # Prepare description to simulate cursor.description
            self.description = [(col,) for col in 
                ["sensor_id", "city", "station", "window_start", "window_end", "temperature", "humidity"]]
        def execute(self, query, params):
            # Record the query and parameters passed
            self.query = query
            self.params = params
            # Use the parameters to simulate a result row covering the full 24h window
            start_time = params[0]
            end_time = params[1]
            # Create one dummy row of data
            self.rows = [
                (1, "CityA", "StationX", start_time, end_time, 20.0, 30.0)
            ]
        def fetchall(self):
            return getattr(self, 'rows', [])
        def close(self):
            self.closed = True

    class DummyConn:
        def __init__(self):
            self.closed = False
        def close(self):
            self.closed = True

    dummy_cursor = DummyCursor()
    dummy_conn = DummyConn()
    def dummy_get_connection():
        # Return dummy connection and cursor as a tuple
        return dummy_conn, dummy_cursor

    # Monkeypatch the database get_connection function
    monkeypatch.setattr(extract, 'get_connection', dummy_get_connection)

    # Define a test date and call the extract function
    date_str = '2025-01-01'
    df = extract.extract_sensor_readings(date_str)

    # After extraction, the database connection should be closed
    assert dummy_conn.closed, "Database connection should be closed after extraction"
    # Verify that the SQL was executed with correct parameters (24-hour window)
    expected_start = datetime.fromisoformat(date_str)
    expected_end = expected_start + timedelta(days=1)
    assert dummy_cursor.params[0] == expected_start and dummy_cursor.params[1] == expected_end, "Query parameters should be the start and end of the 24h window"
    # The result should be a pandas DataFrame with the expected columns, including 'date_str'
    expected_columns = {"sensor_id", "city", "station", "window_start", "window_end", "temperature", "humidity", "date_str"}
    assert set(df.columns) == expected_columns, "Returned DataFrame columns are incorrect"
    # DataFrame should contain one row with data corresponding to our dummy row
    assert len(df) == 1, "DataFrame should have one row of data"
    row = df.iloc[0]
    # Check that the data in the DataFrame matches the dummy data
    assert row['sensor_id'] == 1
    assert row['city'] == "CityA"
    assert row['station'] == "StationX"
    # The date_str column should match the input date string
    assert row['date_str'] == "2025-01-01", "date_str column should be formatted as YYYY-MM-DD"
