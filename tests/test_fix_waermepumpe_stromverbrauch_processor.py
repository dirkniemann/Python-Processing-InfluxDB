from datetime import datetime, timedelta, timezone
import importlib

import pytest


@pytest.fixture()
def processor(fake_influx_module):
    module = importlib.import_module(
        "moduls.processing.fix_waermepumpe_stromverbrauch_processor"
    )
    importlib.reload(module)
    return module.FixWaermepumpeStromverbrauchProcessor(
        influx_handler=object(),
        input_bucket="input",
        output_bucket="output",
        version="v1",
        entities=["pump"],
        first_data_day=datetime(2026, 1, 1).date(),
        output_measurement="fixed",
    )


def test_make_monoton_clamps_small_drop(processor):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    data = [
        {"time": start, "value": 1.0},
        {"time": start + timedelta(hours=1), "value": 0.9},
    ]

    result = processor._make_monoton(data)

    assert result[1]["value"] == 1.0


def test_make_monoton_rejects_large_drop(processor):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    data = [
        {"time": start, "value": 1.0},
        {"time": start + timedelta(hours=1), "value": 0.7},
    ]

    with pytest.raises(ValueError, match="Monotonicity violation"):
        processor._make_monoton(data)


def test_fill_with_zeroes_creates_hourly_points(processor):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)

    result = processor._fill_with_zeroes(start, end)

    assert result == [
        {"time": start + timedelta(hours=1), "value": 0.0},
        {"time": start + timedelta(hours=2), "value": 0.0},
        {"time": start + timedelta(hours=3), "value": 0.0},
    ]


def test_process_day_repairs_realistic_missing_midnight_reset(processor):
    day = datetime(2026, 9, 20).date()
    records = [
        {"time": datetime(2026, 9, 20, 13, 26, 56, 819213, tzinfo=timezone.utc), "value": 0.1},
        {"time": datetime(2026, 9, 20, 13, 39, 56, 145301, tzinfo=timezone.utc), "value": 0.3},
        {"time": datetime(2026, 9, 20, 21, 53, 57, 115718, tzinfo=timezone.utc), "value": 10.4},
        {"time": datetime(2026, 9, 20, 21, 59, 57, 389305, tzinfo=timezone.utc), "value": 0.1},
    ]

    class Handler:
        def __init__(self):
            self.writes = []

        def get_data(self, **kwargs):
            return list(records)

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    processor.influx_handler = Handler()

    processor._process_day(day)

    values = [write["value"] for write in processor.influx_handler.writes]
    times = [write["timestamp"] for write in processor.influx_handler.writes]
    assert values[0] == 0.0
    assert values[-1] == 10.4
    assert values == sorted(values)
    assert times[0].isoformat() == "2026-09-19T22:00:00+00:00"
    assert times[-1].isoformat() == "2026-09-20T21:59:59+00:00"


def test_process_day_fills_missing_data_with_zeroes(processor):
    day = datetime(2026, 9, 20).date()

    class Handler:
        def __init__(self):
            self.writes = []

        def get_data(self, **kwargs):
            return []

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    processor.influx_handler = Handler()

    processor._process_day(day)

    assert len(processor.influx_handler.writes) == 25
    assert all(write["value"] == 0.0 for write in processor.influx_handler.writes)