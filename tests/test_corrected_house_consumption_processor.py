from datetime import date, datetime
import importlib

import pytz
import pytest


def test_process_day_unions_events_and_holds_previous_values(fake_influx_module):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    utc = pytz.UTC
    source_values = {
    "load": [
            {"time": utc.localize(datetime(2026, 1, 1, 23, 0)), "value": 100},
            {"time": utc.localize(datetime(2026, 1, 2, 1, 0)), "value": 120},
        ],
        "pv": [
            {"time": utc.localize(datetime(2026, 1, 1, 23, 30)), "value": 10},
            {"time": utc.localize(datetime(2026, 1, 2, 2, 0)), "value": 20},
        ],
    }

    class Handler:
        def __init__(self):
            self.writes = []

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": utc.localize(datetime(2026, 1, 1, 23, 0)), "value": 100} if kwargs["entity_id"] == "load" else {"time": utc.localize(datetime(2026, 1, 1, 23, 30)), "value": 10}

        def get_data(self, **kwargs):
            return source_values[kwargs["entity_id"]]

        def get_last_data_day(self, **kwargs):
            return date(2026, 1, 1)

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    handler = Handler()
    processor = module.CorrectedHouseConsumptionProcessor(
        influx_handler=handler,
        input_bucket="input",
        output_bucket="output",
        version="v1",
        sources={
            "fems_house_consumption": {
                "bucket": "input", "measurement": "W", "entity_id": "load", "field": "value"
            },
            "mt_stall_neu_power": {
                "bucket": "input", "measurement": "W", "entity_id": "pv", "field": "value"
            },
        },
        first_data_day=date(2026, 1, 1),
        output_measurement="Hausverbrauch_korrigiert",
        output_entity_id="Hausverbrauch_korrigiert",
    )

    processor._process_day(date(2026, 1, 2))

    assert [write["value"] for write in handler.writes] == pytest.approx([110, 110, 130, 140])
    assert [write["timestamp"].hour for write in handler.writes] == [23, 23, 1, 2]


def test_process_day_rejects_missing_start_state(fake_influx_module):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    class Handler:
        def get_latest_datapoint_by_time(self, **kwargs):
            return None

        def get_data(self, **kwargs):
            return []

    processor = module.CorrectedHouseConsumptionProcessor(
        influx_handler=Handler(),
        input_bucket="input",
        output_bucket="output",
        version="v1",
        sources={
            "fems_house_consumption": {"bucket": "input", "measurement": "W", "entity_id": "load", "field": "value"},
            "mt_stall_neu_power": {"bucket": "input", "measurement": "W", "entity_id": "pv", "field": "value"},
        },
        first_data_day=date(2026, 1, 1),
        output_measurement="Hausverbrauch_korrigiert",
        output_entity_id="Hausverbrauch_korrigiert",
    )

    with pytest.raises(ValueError, match="No input data"):
        processor._process_day(date(2026, 1, 2))


def test_write_value_clamps_negative_house_consumption_and_warns(fake_influx_module, caplog):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    class Handler:
        def __init__(self):
            self.write = None

        def write_datapoint(self, **kwargs):
            self.write = kwargs

    handler = Handler()
    processor = module.CorrectedHouseConsumptionProcessor(
        influx_handler=handler,
        input_bucket="input",
        output_bucket="output",
        version="v1",
        sources={
            "fems_house_consumption": {
                "bucket": "input", "measurement": "W", "entity_id": "load", "field": "value"
            },
            "mt_stall_neu_power": {
                "bucket": "input", "measurement": "W", "entity_id": "pv", "field": "value"
            },
        },
        first_data_day=date(2026, 1, 1),
        output_measurement="Hausverbrauch_korrigiert",
        output_entity_id="Hausverbrauch_korrigiert",
    )

    with caplog.at_level("WARNING"):
        processor._write_value(
            pytz.UTC.localize(datetime(2026, 1, 2, 1)),
            {"fems_house_consumption": -5, "mt_stall_neu_power": 2},
        )

    assert handler.write["value"] == 0.0
    assert "below zero" in caplog.text