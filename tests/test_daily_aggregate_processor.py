from datetime import date, datetime
import importlib

import pytest


def test_process_day_writes_entity_and_total_sums(fake_influx_module):
    module = importlib.import_module("moduls.processing.daily_aggregate_processor")
    importlib.reload(module)

    class Handler:
        def __init__(self):
            self.writes = []

        def get_last_datapoint(self, **kwargs):
            return {"value": {"one": 1.25, "two": 2.75}[kwargs["entity_id"]]}

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    handler = Handler()
    processor = module.DailyAggregateProcessor(
        influx_handler=handler,
        input_bucket="input",
        output_bucket="output",
        version="v1",
        entities=["one", "two"],
        first_data_day=date(2026, 1, 1),
        output_measurement="daily",
        output_entity_id="total",
    )

    processor._process_day(date(2026, 1, 2), "fixed-v1")

    assert [write["entity_id"] for write in handler.writes] == ["one", "two", "total"]
    assert [write["value"] for write in handler.writes] == pytest.approx([1.25, 2.75, 4.0])
    assert all(write["field"] == "daily_sum" for write in handler.writes)
    assert all(write["version"] == "v1" for write in handler.writes)
    assert all(isinstance(write["timestamp"], datetime) for write in handler.writes)


def test_process_day_fails_when_an_entity_has_no_data(fake_influx_module):
    module = importlib.import_module("moduls.processing.daily_aggregate_processor")
    importlib.reload(module)

    class Handler:
        def get_last_datapoint(self, **kwargs):
            return None

    processor = module.DailyAggregateProcessor(
        influx_handler=Handler(),
        input_bucket="input",
        output_bucket="output",
        version="v1",
        entities=["one"],
        first_data_day=date(2026, 1, 1),
        output_entity_id="total",
    )

    with pytest.raises(ValueError, match="No data found for one"):
        processor._process_day(date(2026, 1, 2), "fixed-v1")


def test_process_processes_one_pending_day(fake_influx_module, monkeypatch):
    module = importlib.import_module("moduls.processing.daily_aggregate_processor")
    importlib.reload(module)
    day = date(2026, 1, 2)

    class Handler:
        def __init__(self):
            self.writes = []

        def get_last_data_day(self, **kwargs):
            return None

        def get_last_version(self, **kwargs):
            return "fixed-v1"

        def get_last_datapoint(self, **kwargs):
            return {"value": 2.5}

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    monkeypatch.setattr(module, "get_days_to_process", lambda last_day: [day])
    handler = Handler()
    processor = module.DailyAggregateProcessor(
        influx_handler=handler,
        input_bucket="input",
        output_bucket="output",
        version="v1",
        entities=["one"],
        first_data_day=day,
        output_measurement="daily",
        output_entity_id="total",
    )

    assert processor.process() == 1
    assert [write["entity_id"] for write in handler.writes] == ["one", "total"]
    assert all(write["value"] == 2.5 for write in handler.writes)