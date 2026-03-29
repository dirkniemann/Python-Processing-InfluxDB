import importlib
from datetime import date, datetime, timedelta

import pytest


def test_homeassistant_processor_creates_processors(monkeypatch, prod_config, fake_influx_module):
    processing_module = importlib.import_module("moduls.processing.HomeAssistant_processing")
    importlib.reload(processing_module)

    created = []

    def make_stub(name):
        class StubProcessor:
            def __init__(self, *args, **kwargs):
                self.name = name
                self.args = args
                self.kwargs = kwargs
                self.process_called = False
                created.append(self)

            def process(self):
                self.process_called = True

        return StubProcessor

    monkeypatch.setattr(processing_module, "FixWaermepumpeStromverbrauchProcessor", make_stub("fix"))
    monkeypatch.setattr(processing_module, "DailyAggregateProcessor", make_stub("daily"))
    monkeypatch.setattr(processing_module, "WaermepumpeStatistikProcessor", make_stub("stat"))

    handler = object()
    first_day = datetime(2024, 1, 1).date()
    processor = processing_module.HomeAssistantProcessor(
        influx_handler=handler,
        processing_config=prod_config["processing"],
        first_data_day=first_day,
    )

    assert len(processor.processors) == 3
    processor.process_data()
    assert all(instance.process_called for instance in created)

    fix_stub = next(p for p in created if p.name == "fix")
    daily_stub = next(p for p in created if p.name == "daily")
    stat_stub = next(p for p in created if p.name == "stat")

    assert fix_stub.kwargs["output_measurement"] == prod_config["processing"]["entities_to_process"]["fix_waermepumpe_stromverbrauch"]["output_measurement"]
    assert daily_stub.kwargs["output_entity_id"] == prod_config["processing"]["entities_to_process"]["daily_aggregate"]["output_entity_id"]
    assert stat_stub.kwargs["output_entity_id"] == prod_config["processing"]["entities_to_process"]["Waermepumpe_statistik"]["output_entity_id"]


def test_homeassistant_processor_validates_config(monkeypatch, fake_influx_module):
    processing_module = importlib.import_module("moduls.processing.HomeAssistant_processing")
    importlib.reload(processing_module)

    handler = object()
    bad_config = {"input_bucket": "a", "output_bucket": "b"}
    first_day = datetime(2024, 1, 1).date()

    with pytest.raises(ValueError) as excinfo:
        processing_module.HomeAssistantProcessor(handler, bad_config, first_day)

    assert "Missing required config keys" in str(excinfo.value)


def test_daily_aggregate_requires_output_entity(monkeypatch, fake_influx_module):
    processing_module = importlib.import_module("moduls.processing.HomeAssistant_processing")
    importlib.reload(processing_module)

    handler = object()
    config = {
        "input_bucket": "input",
        "output_bucket": "output",
        "entities_to_process": {
            "daily_aggregate": {
                "version": "v1",
                "entities": ["one"],
            }
        },
    }
    first_day = datetime(2024, 1, 1).date()

    with pytest.raises(ValueError) as excinfo:
        processing_module.HomeAssistantProcessor(handler, config, first_day)

    assert "output_entity_id" in str(excinfo.value)


def test_get_days_to_process_uses_dates(fake_influx_module):
    processing_module = importlib.import_module("moduls.processing.HomeAssistant_processing")
    importlib.reload(processing_module)

    last_day = datetime.now().date() - timedelta(days=1)
    days = processing_module.get_days_to_process(last_day)

    assert isinstance(days, list)
    assert all(isinstance(day, date) for day in days)
    assert not days, "Yesterday should produce no days to process"
