import importlib
from datetime import datetime
import pytz
import pytest


def test_local_to_utc_and_back(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    naive = datetime(2024, 1, 1, 12, 0, 0)
    utc_dt = handler_module.local_to_utc(naive)
    assert utc_dt.tzinfo == pytz.UTC
    round_trip = handler_module.utc_to_local(utc_dt)
    assert round_trip.tzinfo.zone == handler_module.LOCAL_TZ.zone


def test_local_to_utc_rejects_ambiguous_and_nonexistent_dst_times(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)

    for local_time in (
        datetime(2026, 3, 29, 2, 30),
        datetime(2026, 10, 25, 2, 30),
    ):
        with pytest.raises((pytz.NonExistentTimeError, pytz.AmbiguousTimeError)):
            handler_module.local_to_utc(local_time)


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
    assert result["time"].tzinfo == handler_module.UTC_TZ


def test_data_day_uses_berlin_calendar_date(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    utc_before_local_midnight = pytz.UTC.localize(datetime(2026, 1, 1, 23, 30))
    handler.client.query_api_obj.tables = [
        type(
            "Table",
            (),
            {
                "records": [
                    type(
                        "Record",
                        (),
                        {
                            "get_time": lambda self: utc_before_local_midnight,
                        },
                    )()
                ]
            },
        )()
    ]

    assert handler.get_first_data_day(bucket="input") == datetime(2026, 1, 2).date()
    assert handler.get_last_data_day(bucket="input", version="v1") == datetime(2026, 1, 2).date()


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
    assert record["record"]["fields"]["f"] == 1.0


def test_write_fields_datapoints_writes_bounded_batches(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()

    count = handler.write_fields_datapoints(
        bucket="scenario-output",
        measurement="batterie_szenarien",
        batch_size=2,
        datapoints=[
            {
                "fields": {"soc_pct": 50 + index},
                "tags": {"version": "v5", "entity_id": "soc_pct", "unit": "%"},
                "timestamp": datetime(2026, 1, 1, 0, index),
            }
            for index in range(3)
        ],
    )

    assert count == 3
    writes = handler.client._write_api.records
    assert [len(write["record"]) for write in writes] == [2, 1]
    assert all(write["bucket"] == "scenario-output" for write in writes)
    assert all(point["measurement"] == "batterie_szenarien" for write in writes for point in write["record"])
    assert all(point["fields"]["soc_pct"] >= 50 for write in writes for point in write["record"])


def test_get_data_converts_naive_to_utc(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()

    class CapturingQueryAPI:
        def __init__(self):
            self.last_query = None

        def query(self, query, *_, **__):
            self.last_query = query
            return []

    capturing_api = CapturingQueryAPI()
    handler.client.query_api_obj = capturing_api

    handler.get_data(
        start_time=datetime(2024, 1, 1, 12, 0, 0),
        stop_time=datetime(2024, 1, 1, 13, 0, 0),
        bucket="bucket",
        entity_id="sensor.demo",
    )

    assert capturing_api.last_query is not None
    assert "+00:00" in capturing_api.last_query, "Expected UTC isoformat timestamps"


def test_scenario_daily_records_merge_entity_fields_without_local_day(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    timestamp = pytz.UTC.localize(datetime(2026, 1, 1, 22, 59, 59))

    class Record:
        def __init__(self, entity, field, value):
            self.values = {"entity_id": entity, "_field": field}
            self.value = value

        def get_time(self):
            return timestamp

        def get_value(self):
            return self.value

    handler.client.query_api_obj.tables = [
        type("Table", (), {"records": [
            Record("grid_import", "daily_sum", 4.5),
            Record("grid_import", "quality", 1.5),
            Record("grid_import", "signed_error", -1.5),
            Record("soc_pct", "start", 30.0),
            Record("soc_pct", "end", 35.0),
            Record("soc_pct", "quality", 2.5),
            Record("soc_pct", "signed_error", -0.5),
            Record("eta_charge", "daily_value", 0.91),
            Record("stored_energy", "end", 7.0),
        ]})()
    ]

    records = handler.get_scenario_daily_records(
        bucket="testing",
        scenario="current_battery",
        pv_mode="without_old_pv",
        version="v5",
    )

    assert records == [
        {
            "time": timestamp,
            "grid_import_kwh": 4.5,
            "grid_import_quality": 1.5,
            "grid_import_signed_error": -1.5,
            "soc_start_pct": 30.0,
            "soc_end_pct": 35.0,
            "soc_pct_quality": 2.5,
            "soc_pct_signed_error": -0.5,
            "eta_charge": 0.91,
            "stored_energy_end_kwh": 7.0,
        }
    ]


def test_scenario_timeseries_records_merge_actual_fields_by_timestamp(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    timestamp = pytz.UTC.localize(datetime(2026, 1, 1, 12))

    class Record:
        def __init__(self, entity, value, field="actual"):
            self.values = {"entity_id": entity, "_field": field}
            self.value = value

        def get_time(self):
            return timestamp

        def get_value(self):
            return self.value

    handler.client.query_api_obj.tables = [
        type("Table", (), {"records": [
            Record("soc_pct", 50.0),
            Record("pv_to_battery", 1000.0),
            Record("battery_to_load", 500.0),
            Record("grid_import", 100.0, "error"),
            Record("grid_export", -50.0, "error"),
        ]})()
    ]

    records = handler.get_scenario_timeseries_records(
        bucket="testing",
        scenario="current_battery",
        pv_mode="without_old_pv",
        version="v5",
    )

    assert records == [
        {
            "time": timestamp,
            "soc_pct": 50.0,
            "battery_charge_dc_kw": 1.0,
            "battery_discharge_dc_kw": 0.5,
            "grid_import_error_w": 100.0,
            "grid_export_error_w": -50.0,
        }
    ]


def test_get_last_datapoint_returns_none_on_missing_data(fake_influx_module):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    handler.client.query_api_obj.tables = []

    result = handler.get_last_datapoint(start_time=datetime(2024, 1, 1), bucket="b", entity_id="e")

    assert result is None
