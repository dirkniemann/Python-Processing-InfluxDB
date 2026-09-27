import argparse
import importlib
from datetime import datetime


class FakeHandler:
    def __init__(self, *_, **__):
        self.connected = False
        self.disconnected = False

    def connect(self):
        self.connected = True
        return True

    def disconnect(self):
        self.disconnected = True

    def get_first_data_day(self, bucket):
        return datetime(2024, 1, 1).date()


class FakeProcessor:
    def __init__(self, influx_handler, processing_config, first_data_day):
        self.called = False

    def process_data(self):
        self.called = True


class InterruptingProcessor(FakeProcessor):
    def process_data(self):
        raise KeyboardInterrupt


def test_main_runs_with_fakes(monkeypatch):
    main_module = importlib.import_module("main")
    importlib.reload(main_module)
    summaries = []

    monkeypatch.setattr(main_module, "parse_arguments", lambda: argparse.Namespace(stage="prod", log_level="INFO", log_file=None))
    monkeypatch.setattr(main_module, "load_configuration", lambda stage: {"mqtt": {"enabled": False}, "processing": {"input_bucket": "i", "output_bucket": "o", "entities_to_process": {}}})
    monkeypatch.setattr(main_module, "setup_environment", lambda stage: None)
    monkeypatch.setattr(main_module, "InfluxDBHandler", FakeHandler)
    monkeypatch.setattr(main_module, "HomeAssistantProcessor", FakeProcessor)
    monkeypatch.setattr(main_module, "get_logger", lambda **kwargs: importlib.import_module("logging").getLogger("main-test"))
    monkeypatch.setattr(main_module, "write_run_summary", lambda **kwargs: summaries.append(kwargs))

    exit_code = main_module.main()
    assert exit_code == 0
    assert summaries[0]["status"] == "SUCCESS"


def test_main_reports_keyboard_interrupt(monkeypatch):
    main_module = importlib.import_module("main")
    importlib.reload(main_module)
    summaries = []

    monkeypatch.setattr(main_module, "parse_arguments", lambda: argparse.Namespace(stage="prod", log_level="INFO", log_file=None))
    monkeypatch.setattr(main_module, "load_configuration", lambda stage: {"mqtt": {"enabled": False}, "processing": {"input_bucket": "i", "output_bucket": "o", "entities_to_process": {}}})
    monkeypatch.setattr(main_module, "setup_environment", lambda stage: None)
    monkeypatch.setattr(main_module, "InfluxDBHandler", FakeHandler)
    monkeypatch.setattr(main_module, "HomeAssistantProcessor", InterruptingProcessor)
    monkeypatch.setattr(main_module, "get_logger", lambda **kwargs: importlib.import_module("logging").getLogger("main-interrupt-test"))
    monkeypatch.setattr(main_module, "write_run_summary", lambda **kwargs: summaries.append(kwargs))

    exit_code = main_module.main()

    assert exit_code == 130
    assert summaries[0]["status"] == "ERROR"
    assert summaries[0]["error_type"] == "KeyboardInterrupt"
    assert summaries[0]["error_message"] == "Lauf manuell abgebrochen"


def test_main_reports_connection_failure(monkeypatch):
    main_module = importlib.import_module("main")
    importlib.reload(main_module)
    summaries = []

    monkeypatch.setattr(main_module, "parse_arguments", lambda: argparse.Namespace(stage="prod", log_level="INFO", log_file=None))
    monkeypatch.setattr(main_module, "setup_environment", lambda stage: None)
    monkeypatch.setattr(main_module, "InfluxDBHandler", lambda: type("Handler", (), {"connect": lambda self: False, "disconnect": lambda self: None})())
    monkeypatch.setattr(main_module, "load_configuration", lambda stage: {"mqtt": {"enabled": False}, "processing": {"input_bucket": "input"}})
    monkeypatch.setattr(main_module, "get_logger", lambda **kwargs: importlib.import_module("logging").getLogger("main-connection-error"))
    monkeypatch.setattr(main_module, "write_run_summary", lambda **kwargs: summaries.append(kwargs))

    assert main_module.main() == 1
    assert summaries[0]["status"] == "ERROR"
    assert summaries[0]["step"] == "InfluxDB connection"
    assert summaries[0]["error_type"] == "RuntimeError"
