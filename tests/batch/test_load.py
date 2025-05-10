import batch.load as load

def test_load_reports(monkeypatch):
    """Test that load_reports upserts each report into MongoDB with correct data and closes the client."""
    # Remove any existing MONGO_URI env var to use default
    monkeypatch.delenv('MONGO_URI', raising=False)
    # Dummy MongoDB collection to capture replace_one calls
    class DummyCollection:
        def __init__(self):
            self.replace_calls = []  # Store (filter, document, upsert) for each call
        def replace_one(self, filter_doc, new_doc, upsert=False):
            # Record the filter criteria, document, and upsert flag
            self.replace_calls.append((filter_doc, new_doc, upsert))
    class DummyDB:
        def __init__(self):
            self.reports = DummyCollection()
    class DummyClient:
        def __init__(self, uri):
            self.uri = uri
            self.sensor_data_batch = DummyDB()
            self.closed = False
        def close(self):
            # Mark the client as closed when called
            self.closed = True

    # Monkeypatch MongoClient to use our DummyClient
    dummy_client_instance = None
    def dummy_mongo_client(uri):
        nonlocal dummy_client_instance
        dummy_client_instance = DummyClient(uri)
        return dummy_client_instance
    monkeypatch.setattr(load, 'MongoClient', dummy_mongo_client)

    # Sample reports list (mix of string and int sensor_id to test conversion)
    reports = [
        {
            'sensor_id': '1',
            'city': 'CityA',
            'station': 'S1',
            'date': '2025-01-01',
            'data': {'value': 100}
        },
        {
            'sensor_id': 2,
            'city': 'CityB',
            'station': 'S2',
            'date': '2025-01-02',
            'data': {'value': 200}
        }
    ]
    # Call the function under test
    load.load_reports(reports)

    # The dummy MongoClient should have been created with default URI (since env var is not set)
    assert dummy_client_instance is not None, "MongoClient should be instantiated"
    assert dummy_client_instance.uri == 'mongodb://localhost:27017', "Should use default Mongo URI if none is set in env"
    # After function execution, the client should be closed
    assert dummy_client_instance.closed, "MongoClient should be closed after loading reports"

    # Verify that replace_one was called for each report with correct filter and document
    calls = dummy_client_instance.sensor_data_batch.reports.replace_calls
    assert len(calls) == len(reports), "replace_one should be called once per report"
    # Check details of the first call
    filter1, doc1, upsert1 = calls[0]
    import datetime
    expected_date1 = datetime.datetime.strptime(reports[0]['date'], '%Y-%m-%d')
    # Filter should have sensor_id as int and date as datetime
    assert filter1 == {'sensor_id': 1, 'date': expected_date1}, "Filter criteria for first report is incorrect"
    # Document should contain converted sensor_id (int) and date (datetime), and match other fields
    assert doc1['sensor_id'] == 1
    assert doc1['city'] == 'CityA'
    assert doc1['station'] == 'S1'
    assert doc1['date'] == expected_date1
    assert doc1['data'] == reports[0]['data']
    # Upsert should be True to insert if not present
    assert upsert1 is True
    # Check details of the second call similarly
    filter2, doc2, upsert2 = calls[1]
    expected_date2 = datetime.datetime.strptime(reports[1]['date'], '%Y-%m-%d')
    assert filter2 == {'sensor_id': 2, 'date': expected_date2}, "Filter criteria for second report is incorrect"
    assert doc2['sensor_id'] == 2
    assert doc2['city'] == 'CityB'
    assert doc2['station'] == 'S2'
    assert doc2['date'] == expected_date2
    assert doc2['data'] == reports[1]['data']
    assert upsert2 is True
