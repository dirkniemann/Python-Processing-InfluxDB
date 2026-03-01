import importlib
from datetime import datetime
import pytz


def test_homeassistant_processor_processes_daily_aggregate(fake_influx_module, prod_config, fake_logger):
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    processing_module = importlib.import_module("moduls.HomeAssistant_processing")
    importlib.reload(processing_module)

    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()

    # Track writes
    writes = []
    handler.write_datapoint = lambda **kwargs: writes.append(kwargs) or True
    handler.get_last_data_day = lambda bucket, entity_id, version=None, scenario=None: None
    handler.get_last_datapoint = lambda start_time, bucket, entity_id, field="value": {"value": 2}

    first_data_day = handler_module.LOCAL_TZ.localize(datetime(2024, 1, 1))
    processor = processing_module.HomeAssistantProcessor(
        influx_handler=handler,
        processing_config=prod_config["processing"],
        first_data_day=first_data_day,
    )

    processor.process_data()
    assert writes, "Expected at least one write during processing"


def test_homeassistant_processor_validates_config(fake_influx_module):
    processing_module = importlib.import_module("moduls.HomeAssistant_processing")
    importlib.reload(processing_module)
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)

    handler = handler_module.InfluxDBHandler()
    handler.client = handler_module.InfluxDBClient()
    bad_config = {"input_bucket": "a"}
    first_day = pytz.UTC.localize(datetime(2024, 1, 1))
    try:
        processing_module.HomeAssistantProcessor(handler, bad_config, first_day)
    except ValueError as exc:
        assert "Missing required config keys" in str(exc)
    else:
        assert False, "Expected ValueError for missing keys"
