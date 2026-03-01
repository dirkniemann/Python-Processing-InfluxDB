import importlib
from datetime import datetime
import pytz


def test_local_to_utc_and_back(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    naive = datetime(2024, 1, 1, 12, 0, 0)
    utc_dt = handler_module.local_to_utc(naive)
    assert utc_dt.tzinfo == pytz.UTC
    round_trip = handler_module.utc_to_local(utc_dt)
    assert round_trip.tzinfo.zone == handler_module.LOCAL_TZ.zone


def test_connect_uses_fake_client(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    connected = handler.connect()
    assert connected is True
    assert handler.client is not None


def test_get_last_datapoint_returns_latest(fake_influx_module, fake_tz_datetime):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    table = type("Table", (), {"records": [type("Record", (), {"get_time": lambda self: fake_tz_datetime, "get_value": lambda self: 7})()]})()
    handler.client.query_api_obj.tables = [table]
    result = handler.get_last_datapoint(start_time=datetime(2024, 1, 1), bucket="b", entity_id="e")
    assert result["value"] == 7
    assert result["time"].tzinfo.zone == handler_module.LOCAL_TZ.zone


def test_write_datapoint_writes_record(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    ok = handler.write_datapoint(bucket="b", entity_id="e", field="f", value=1, timestamp=datetime(2024, 1, 1))
    assert ok is True
    assert handler.client._write_api.records
    record = handler.client._write_api.records[0]
    assert record["bucket"] == "b"
    assert record["record"]["fields"]["f"] == 1
