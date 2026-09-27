from datetime import date, datetime, timedelta
import importlib

import pytz
import pytest


def test_process_day_writes_time_weighted_interval_means(fake_influx_module):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    utc = pytz.UTC
    source_values = {
        "load": [
            {"time": utc.localize(datetime(2026, 1, 1, 23, 0)), "value": 100},
            {"time": utc.localize(datetime(2026, 1, 2, 1, 2)), "value": 120},
        ],
        "pv": [
            {"time": utc.localize(datetime(2026, 1, 1, 23, 30)), "value": 10},
            {"time": utc.localize(datetime(2026, 1, 2, 2, 1)), "value": 20},
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

    values = [write["value"] for write in handler.writes]
    timestamps = [write["timestamp"] for write in handler.writes]

    assert len(handler.writes) == 288
    assert values[:24] == pytest.approx([110] * 24)
    assert values[24] == pytest.approx(122)
    assert values[25:36] == pytest.approx([130] * 11)
    assert values[36] == pytest.approx(138)
    assert values[37:] == pytest.approx([140] * (288 - 37))
    assert timestamps[0] == utc.localize(datetime(2026, 1, 1, 23, 0))
    assert timestamps[24] == utc.localize(datetime(2026, 1, 2, 1, 0))
    assert timestamps[36] == utc.localize(datetime(2026, 1, 2, 2, 0))
    assert all(
        (later - earlier).total_seconds() == 300
        for earlier, later in zip(timestamps, timestamps[1:])
    )


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


def test_process_first_day_skips_bins_until_all_source_states_are_known(fake_influx_module, caplog):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    utc = pytz.UTC
    day_start = utc.localize(datetime(2026, 1, 1, 23, 0))
    source_values = {
        "load": [
            {"time": day_start + timedelta(minutes=2), "value": 100}
        ],
        "pv": [
            {"time": day_start + timedelta(minutes=3), "value": 10}
        ],
    }

    class Handler:
        def __init__(self):
            self.writes = []

        def get_latest_datapoint_by_time(self, **kwargs):
            return None

        def get_data(self, **kwargs):
            return source_values[kwargs["entity_id"]]

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
        first_data_day=date(2026, 1, 2),
        output_measurement="Hausverbrauch_korrigiert",
        output_entity_id="Hausverbrauch_korrigiert",
    )

    with caplog.at_level("WARNING"):
        processor._process_day(date(2026, 1, 2))

    assert len(handler.writes) == 287
    assert handler.writes[0]["timestamp"] == day_start + timedelta(minutes=5)
    assert handler.writes[0]["value"] == pytest.approx(110)
    assert "Skipped 1 initial interval" in caplog.text


def test_process_day_skips_when_no_full_interval_has_source_coverage(fake_influx_module, caplog):
    module = importlib.import_module(
        "moduls.processing.corrected_house_consumption_processor"
    )
    importlib.reload(module)

    utc = pytz.UTC
    day_start = utc.localize(datetime(2026, 1, 1, 23, 0))
    first_value_time = day_start + timedelta(hours=23, minutes=59)
    source_values = {
        "load": [{"time": first_value_time, "value": 100}],
        "pv": [{"time": first_value_time, "value": 10}],
    }

    class Handler:
        def __init__(self):
            self.writes = []

        def get_latest_datapoint_by_time(self, **kwargs):
            return None

        def get_data(self, **kwargs):
            return source_values[kwargs["entity_id"]]

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
        first_data_day=date(2026, 1, 2),
        output_measurement="Hausverbrauch_korrigiert",
        output_entity_id="Hausverbrauch_korrigiert",
    )

    with caplog.at_level("WARNING"):
        processor._process_day(date(2026, 1, 2))

    assert handler.writes == []
    assert "No complete 300-second intervals" in caplog.text


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
