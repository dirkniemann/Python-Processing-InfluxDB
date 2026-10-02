from datetime import datetime
import importlib

import pytest


def test_handler_requires_all_credentials(monkeypatch, fake_influx_module):
    module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(module)
    monkeypatch.delenv("INFLUX_TOKEN", raising=False)

    with pytest.raises(ValueError, match="INFLUX_TOKEN"):
        module.InfluxDBHandler()


def test_get_last_version_uses_highest_numeric_suffix(fake_influx_module):
    module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(module)
    handler = module.InfluxDBHandler()
    handler.client = module.InfluxDBClient()

    class Record:
        def __init__(self, value):
            self.value = value

        def get_value(self):
            return self.value

    table = type("Table", (), {"records": [Record("v2"), Record("v10"), Record("legacy")]})()
    handler.client.query_api_obj.tables = [table]

    assert handler.get_last_version(bucket="output") == "v10"


def test_get_data_returns_sorted_records(fake_influx_module):
    module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(module)
    handler = module.InfluxDBHandler()
    handler.client = module.InfluxDBClient()
    later = module.UTC_TZ.localize(datetime(2024, 1, 1, 13))
    earlier = module.UTC_TZ.localize(datetime(2024, 1, 1, 12))
    handler.client.query_api_obj.tables = [
        type(
            "Table",
            (),
            {
                "records": [
                    type("Record", (), {"get_time": lambda self: later, "get_value": lambda self: 2})(),
                    type("Record", (), {"get_time": lambda self: earlier, "get_value": lambda self: 1})(),
                ]
            },
        )()
    ]

    result = handler.get_data(
        start_time=datetime(2024, 1, 1),
        bucket="input",
        entity_id="pump",
    )

    assert [record["value"] for record in result] == [1, 2]


@pytest.mark.parametrize(
    "method, kwargs",
    [
        (
            "get_last_datapoint",
            {"start_time": datetime(2024, 1, 1), "bucket": "input", "entity_id": "pump"},
        ),
        ("get_first_data_day", {"bucket": "input"}),
        (
            "get_last_data_day",
            {"bucket": "output", "version": "v1"},
        ),
    ],
)
def test_query_errors_are_not_misreported_as_missing_data(
    fake_influx_module, method, kwargs
):
    module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(module)
    handler = module.InfluxDBHandler()
    handler.client = module.InfluxDBClient()

    class FailingQueryAPI:
        def query(self, *_, **__):
            raise ConnectionError("database unavailable")

    handler.client.query_api_obj = FailingQueryAPI()

    with pytest.raises(RuntimeError, match="database unavailable"):
        getattr(handler, method)(**kwargs)


@pytest.mark.parametrize(
    "method, kwargs",
    [
        ("get_data", {"start_time": datetime(2024, 1, 1), "bucket": "input", "entity_id": "pump"}),
        ("get_scenario_daily_records", {"bucket": "output", "scenario": "battery", "pv_mode": "mode", "version": "v1"}),
        ("get_scenario_timeseries_records", {"bucket": "output", "scenario": "battery", "pv_mode": "mode", "version": "v1"}),
    ],
)
def test_missing_client_is_a_runtime_error(fake_influx_module, method, kwargs):
    module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(module)
    handler = module.InfluxDBHandler()

    with pytest.raises(RuntimeError, match="client not connected"):
        getattr(handler, method)(**kwargs)