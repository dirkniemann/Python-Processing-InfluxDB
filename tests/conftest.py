"""Shared pytest fixtures and fakes."""
import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime
import importlib
import pytz
import pytest

# Base paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
CONFIG_PATH = ROOT_DIR / "config" / "prod.json"


class FakeWriteAPI:
    """Collect writes for assertions."""

    def __init__(self):
        self.records = []

    def write(self, bucket=None, org=None, record=None, **kwargs):
        self.records.append({"bucket": bucket, "org": org, "record": record, "kwargs": kwargs})


class FakeRecord:
    """Mimic a minimal Influx record."""

    def __init__(self, time: datetime, value):
        self._time = time
        self._value = value

    def get_time(self):
        return self._time

    def get_value(self):
        return self._value


class FakeTable:
    def __init__(self, records=None):
        self.records = records or []


class FakeQueryAPI:
    def __init__(self, tables=None):
        self.tables = tables or []

    def query(self, *_, **__):
        return self.tables


class FakeInfluxDBClient:
    """Simple stand-in for influxdb_client.InfluxDBClient."""

    def __init__(self, *_, **__):
        self._write_api = FakeWriteAPI()
        self.query_api_obj = FakeQueryAPI()

    def health(self):
        return SimpleNamespace(message="ok")

    def query_api(self):
        return self.query_api_obj

    def write_api(self, *_, **__):
        return self._write_api

    def close(self):
        return None


class FakePoint:
    def __init__(self, measurement):
        self.measurement = measurement
        self.tags = {}
        self.fields = {}
        self.time_value = None

    def tag(self, key, value):
        self.tags[key] = value
        return self

    def field(self, key, value):
        self.fields[key] = value
        return self

    def time(self, value, *_, **__):
        self.time_value = value
        return self


@pytest.fixture(scope="session")
def prod_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(autouse=True, scope="session")
def add_src_to_path():
    """Ensure src/ is importable as a package root."""
    src_str = str(SRC_DIR)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
    return src_str


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    """Provide the env vars required by InfluxDBHandler."""
    monkeypatch.setenv("INFLUX_URL", "http://localhost")
    monkeypatch.setenv("INFLUX_TOKEN", "token")
    monkeypatch.setenv("INFLUX_ORG", "org")


@pytest.fixture()
def fake_influx_module(monkeypatch):
    """Patch influxdb_client import with fakes before module import."""
    fake_module = SimpleNamespace(
        InfluxDBClient=FakeInfluxDBClient,
        Point=FakePoint,
        WritePrecision=SimpleNamespace(NS="ns", S="s", US="us", MS="ms"),
    )
    monkeypatch.setitem(sys.modules, "influxdb_client", fake_module)
    monkeypatch.setitem(sys.modules, "influxdb_client.client.write_api", SimpleNamespace(SYNCHRONOUS="syn"))
    return fake_module


@pytest.fixture()
def fake_tz_datetime():
    return pytz.UTC.localize(datetime(2024, 1, 1, 12, 0, 0))


@pytest.fixture()
def fake_tables(fake_tz_datetime):
    table = FakeTable(records=[FakeRecord(fake_tz_datetime, 42)])
    return [table]


@pytest.fixture()
def connected_handler(fake_influx_module, fake_tables):
    """Connected InfluxDBHandler with fake client and predefined query results."""
    handler_module = importlib.import_module("moduls.influxdb_handler")
    importlib.reload(handler_module)
    handler_cls = handler_module.InfluxDBHandler
    handler = handler_cls()
    handler.client = FakeInfluxDBClient()
    handler.client.query_api_obj.tables = fake_tables
    return handler


@pytest.fixture()
def fake_logger():
    logger = logging.getLogger("pytest")
    logger.handlers = []
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
