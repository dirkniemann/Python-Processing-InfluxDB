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