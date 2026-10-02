from datetime import date, datetime, timedelta
import importlib

import pytz
import pytest


def minimal_config():
    return {
        "scenarios": {
            "version": "v1",
            "buckets": {
                "source_bucket": "raw",
                "processing_bucket": "processed",
                "output_bucket": "testing",
            },
            "setup": {
                "base_capacity_kwh": 10,
                "base_charge_power_dc_kw": 5,
                "base_discharge_power_dc_kw": 5,
                "min_soc_pct": 0,
                "max_soc_pct": 100,
                "initial_soc_pct": 50,
                "loss_model": "idealized_no_additional_losses_v1",
            },
            "sources": {
                "corrected_house_load": {
                    "bucket_ref": "processing_bucket",
                    "measurement": "Hausverbrauch_korrigiert",
                    "entity_id": "Hausverbrauch_korrigiert",
                    "field": "value",
                    "version": "v1",
                },
                "fems_pv": {
                    "bucket_ref": "source_bucket",
                    "measurement": "W",
                    "entity_id": "fems_pv",
                    "field": "value",
                },
            },
            "pv_sources": {"local_pv": ["fems_pv"]},
            "pv_modes": {"without_old_pv": ["local_pv"]},
            "definitions": {
                "current_battery": {
                    "enabled": True,
                    "extra_capacity_kwh": 0,
                    "extra_power_kw": 0,
                }
            },
        }
    }


def test_runner_writes_timeseries_and_daily_points(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(minimal_config())
    day_start = pytz.UTC.localize(datetime(2025, 12, 31, 23))

    class Handler:
        def __init__(self):
            self.writes = []

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": day_start, "value": 1000 if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 0}

        def get_data(self, **kwargs):
            return [{"time": day_start, "value": 1000 if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 3000}]

        def write_fields_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    handler = Handler()
    runner = runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1))

    assert runner.process(last_day=date(2026, 1, 1)) == 1
    measurements = [write["measurement"] for write in handler.writes]
    assert set(measurements) == {"batterie_szenarien"}
    timeseries = next(
        write for write in handler.writes
        if write["tags"].get("entity_id") == "pv_to_load"
        and "actual" in write["fields"]
    )
    assert timeseries["tags"]["scenario"] == "current_battery"
    assert timeseries["tags"]["pv_mode"] == "without_old_pv"
    assert timeseries["tags"]["version"] == "v1"
    assert timeseries["tags"]["entity_id"] == "pv_to_load"
    assert timeseries["tags"]["unit"] == "W"
    assert timeseries["fields"] == {"actual": 1000.0}
    assert not {"run_reason", "run_version", "model_version", "record_type"} & timeseries["tags"].keys()
    daily_grid_import = next(
        write for write in handler.writes
        if write["tags"].get("entity_id") == "grid_import"
        and "daily_sum" in write["fields"]
    )
    daily_grid_export = next(
        write for write in handler.writes
        if write["tags"].get("entity_id") == "grid_export"
        and "daily_sum" in write["fields"]
    )
    daily_pv_to_load = next(
        write for write in handler.writes
        if write["tags"].get("entity_id") == "pv_to_load"
        and "daily_sum" in write["fields"]
    )
    daily_soc = next(
        write for write in handler.writes
        if write["tags"].get("entity_id") == "soc_pct"
        and "start" in write["fields"]
    )
    assert daily_grid_import["tags"]["unit"] == "kWh"
    assert daily_grid_import["timestamp"] == pytz.UTC.localize(datetime(2026, 1, 1, 22, 59, 59))
    assert daily_grid_import["fields"].keys() == {"daily_sum"}
    assert daily_grid_export["fields"].keys() == {"daily_sum"}
    assert daily_pv_to_load["fields"]["daily_sum"] == 24.0
    assert daily_grid_export["fields"]["daily_sum"] == 43.0
    assert set(daily_soc["fields"]) == {"start", "end"}
    assert daily_soc["tags"]["unit"] == "%"
    entities = {write["tags"]["entity_id"] for write in handler.writes}
    assert not {"pv_export", "battery_charge_dc", "battery_discharge_dc", "battery_to_grid"} & entities


def test_negative_raw_house_load_is_treated_as_additional_pv(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config_data = minimal_config()
    source = config_data["scenarios"]["sources"]["corrected_house_load"]
    source.pop("version")
    source["allow_negative"] = True
    source["negative_as_pv"] = True
    config = config_module.load_scenario_configuration(config_data)

    class Handler:
        def __init__(self):
            self.writes = []

        def get_scenario_daily_records(self, **kwargs):
            return []

        def get_latest_datapoint_by_time(self, **kwargs):
            value = -1000 if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 0
            return {"time": kwargs["stop_time"], "value": value}

        def get_data(self, **kwargs):
            value = -1000 if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 0
            return [{"time": kwargs["start_time"], "value": value}]

        def write_fields_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    handler = Handler()
    assert runner_module.BatteryScenarioRunner(
        handler, config, date(2026, 1, 1)
    ).process(last_day=date(2026, 1, 1)) == 1

    grid_export = next(
        write
        for write in handler.writes
        if write["tags"].get("entity_id") == "grid_export"
        and "actual" in write["fields"]
    )
    assert grid_export["fields"]["actual"] == pytest.approx(791.6666667)


def test_source_without_version_has_no_version_filter(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    config_data = minimal_config()
    config_data["scenarios"]["sources"]["corrected_house_load"].pop("version")

    configuration = config_module.load_scenario_configuration(config_data)

    assert configuration.sources["corrected_house_load"].version is None


def test_runner_resumes_from_last_complete_day_after_restart(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(minimal_config())

    class Handler:
        def __init__(self):
            self.daily = []

        def get_scenario_daily_records(self, **kwargs):
            return list(self.daily)

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": kwargs["stop_time"], "value": 1000}

        def get_data(self, **kwargs):
            return [{"time": kwargs["start_time"], "value": 1000 if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 0}]

        def write_fields_datapoint(self, **kwargs):
            fields = kwargs["fields"]
            tags = kwargs["tags"]
            if tags.get("entity_id") == "stored_energy" and "end" in fields:
                self.daily.append(
                    {
                        "time": kwargs["timestamp"],
                        "stored_energy_start_kwh": fields["start"],
                        "stored_energy_end_kwh": fields["end"],
                        "version": tags["version"],
                    }
                )

    handler = Handler()
    first_runner = runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1))
    assert first_runner.process(last_day=date(2026, 1, 1)) == 1

    restarted = runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1))
    assert restarted.process(last_day=date(2026, 1, 2)) == 1
    assert len(handler.daily) == 2
    assert handler.daily[1]["version"] == config.version
    assert handler.daily[1]["stored_energy_start_kwh"] == handler.daily[0]["stored_energy_end_kwh"]


def test_runner_uses_configured_version_as_the_reprocessing_boundary(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(minimal_config())

    class Handler:
        def __init__(self):
            self.daily = []
            self.changed = False
            self.input_queries = 0

        def get_scenario_daily_records(self, **kwargs):
            return [record for record in self.daily if record.get("version") == kwargs["version"]]

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": kwargs["stop_time"], "value": 1000}

        def get_data(self, **kwargs):
            self.input_queries += 1
            value = 2000 if self.changed and kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 1000
            return [{"time": kwargs["start_time"], "value": value}]

        def write_fields_datapoint(self, **kwargs):
            fields = kwargs["fields"]
            tags = kwargs["tags"]
            if tags.get("entity_id") == "stored_energy" and "end" in fields:
                self.daily.append(
                    {
                        "time": kwargs["timestamp"],
                        "stored_energy_end_kwh": fields["end"],
                        "version": tags["version"],
                    }
                )

    handler = Handler()
    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(last_day=date(2026, 1, 2))
    original_count = len(handler.daily)
    original_query_count = handler.input_queries
    handler.changed = True

    same_version_days = runner_module.BatteryScenarioRunner(
        handler, config, date(2026, 1, 1)
    ).process(last_day=date(2026, 1, 2))
    assert same_version_days == 0
    assert len(handler.daily) == original_count
    assert handler.input_queries == original_query_count

    changed_config = minimal_config()
    changed_config["scenarios"]["version"] = "v2"
    version_two = config_module.load_scenario_configuration(changed_config)
    reprocessed_days = runner_module.BatteryScenarioRunner(
        handler, version_two, date(2026, 1, 1)
    ).process(last_day=date(2026, 1, 2))
    assert reprocessed_days == 2
    version_two_records = handler.daily[original_count:]
    assert len(version_two_records) == 2
    assert all(record["version"] == "v2" for record in version_two_records)


def test_runner_loads_each_source_once_per_day_for_all_scenarios(fake_influx_module):
    config_data = minimal_config()
    config_data["scenarios"]["sources"]["old_pv"] = {
        "bucket_ref": "source_bucket",
        "measurement": "W",
        "entity_id": "old_pv",
        "field": "value",
    }
    config_data["scenarios"]["pv_sources"]["old_pv"] = ["old_pv"]
    config_data["scenarios"]["pv_modes"]["with_old_pv"] = ["local_pv", "old_pv"]
    config_data["scenarios"]["definitions"]["second_battery"] = {
        "enabled": True,
        "extra_capacity_kwh": 2,
        "extra_power_kw": 1,
    }
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(config_data)
    day = date(2026, 1, 1)

    class Handler:
        def __init__(self):
            self.source_reads = {}
            self.previous_reads = {}
            self.daily_writes = []

        def get_scenario_daily_records(self, **kwargs):
            return []

        def get_latest_datapoint_by_time(self, **kwargs):
            entity = kwargs["entity_id"]
            self.previous_reads[entity] = self.previous_reads.get(entity, 0) + 1
            return None

        def get_data(self, **kwargs):
            entity = kwargs["entity_id"]
            self.source_reads[entity] = self.source_reads.get(entity, 0) + 1
            value = 1000 if entity == "Hausverbrauch_korrigiert" else 500
            return [{"time": kwargs["start_time"], "value": value}]

        def write_fields_datapoint(self, **kwargs):
            if kwargs["tags"].get("entity_id") == "stored_energy" and "end" in kwargs["fields"]:
                self.daily_writes.append(kwargs)

    handler = Handler()
    runner = runner_module.BatteryScenarioRunner(handler, config, day)

    assert runner.process(last_day=date(2026, 1, 2)) == 2
    assert handler.source_reads == {
        "Hausverbrauch_korrigiert": 2,
        "fems_pv": 2,
        "old_pv": 2,
    }
    assert handler.previous_reads == {
        "Hausverbrauch_korrigiert": 1,
        "fems_pv": 1,
        "old_pv": 1,
    }
    assert len(handler.daily_writes) == 8
    assert {
        (write["tags"]["scenario"], write["tags"]["pv_mode"])
        for write in handler.daily_writes
    } == {
        ("current_battery", "without_old_pv"),
        ("current_battery", "with_old_pv"),
        ("second_battery", "without_old_pv"),
        ("second_battery", "with_old_pv"),
    }


def test_runner_skips_day_with_missing_non_change_only_source(fake_influx_module):
    config_data = minimal_config()
    config_data["scenarios"]["sources"]["fems_pv"]["change_only"] = False
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(config_data)

    class Handler:
        def __init__(self):
            self.writes = []

        def get_scenario_daily_records(self, **kwargs):
            return []

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": kwargs["stop_time"], "value": 1000} if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else None

        def get_data(self, **kwargs):
            return [{"time": kwargs["start_time"], "value": 1000}] if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else []

        def write_fields_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    handler = Handler()
    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(last_day=date(2026, 1, 1))
    assert handler.writes == []


def _quality_config(enabled=True, include_second_scenario=False):
    config = minimal_config()
    config["scenarios"]["validation"] = {
        "battery_power": {
            "bucket_ref": "source_bucket",
            "measurement": "W",
            "entity_id": "battery_power",
            "field": "value",
        },
        "battery_soc": {
            "bucket_ref": "source_bucket",
            "measurement": "%",
            "entity_id": "battery_soc",
            "field": "value",
            "change_only": True,
        },
    }
    config["scenarios"]["quality"] = {"enabled": enabled}
    if enabled:
        config["scenarios"]["quality"]["grid_power"] = {
            "bucket_ref": "source_bucket",
            "measurement": "W",
            "entity_id": "grid_power",
            "field": "value",
            "change_only": True,
        }
    if include_second_scenario:
        config["scenarios"]["definitions"]["another_battery"] = {
            "enabled": True,
            "extra_capacity_kwh": 0,
            "extra_power_kw": 0,
        }
    return config


def test_daily_quality_is_written_only_for_current_battery_without_old_pv(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config_data = _quality_config(enabled=True)
    config_data["scenarios"]["pv_modes"]["with_old_pv"] = ["local_pv"]
    config = config_module.load_scenario_configuration(config_data)
    handler = DailyQualityHandler()

    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(
        last_day=date(2026, 1, 1)
    )

    quality_writes = [write for write in handler.writes if "quality" in write["fields"]]
    error_writes = [write for write in handler.writes if "error" in write["fields"]]
    assert quality_writes
    assert error_writes
    assert {write["tags"]["scenario"] for write in quality_writes} == {"current_battery"}
    assert {write["tags"]["pv_mode"] for write in quality_writes} == {"without_old_pv"}
    assert {write["tags"]["pv_mode"] for write in error_writes} == {"without_old_pv"}


class DailyQualityHandler:
    def __init__(self, missing_soc_start=False):
        self.writes = []
        self.actions = []
        self.quality_reads = []
        self.missing_soc_start = missing_soc_start

    def get_scenario_daily_records(self, **kwargs):
        return []

    def get_latest_datapoint_by_time(self, **kwargs):
        entity = kwargs["entity_id"]
        if entity == "battery_soc" and self.missing_soc_start:
            return None
        value = {
            "Hausverbrauch_korrigiert": 1000,
            "fems_pv": 0,
            "battery_soc": 50,
            "grid_power": 1000,
        }.get(entity, 0)
        return {"time": kwargs["stop_time"] - timedelta(seconds=1), "value": value}

    def get_data(self, **kwargs):
        entity = kwargs["entity_id"]
        start = kwargs["start_time"]
        if entity in {"battery_soc", "grid_power"}:
            day = start.astimezone(pytz.timezone("Europe/Berlin")).date()
            self.quality_reads.append((entity, day))
            self.actions.append(("quality_read", entity, day))
            if entity == "battery_soc":
                if self.missing_soc_start:
                    return [{"time": start + timedelta(hours=12), "value": 55}]
                return [{"time": start + timedelta(hours=12), "value": 55}]
            return [{"time": start + timedelta(hours=12), "value": -1000}]
        value = 1000 if entity == "Hausverbrauch_korrigiert" else 0
        return [{"time": start, "value": value}]

    def write_fields_datapoint(self, **kwargs):
        self.writes.append(kwargs)
        tags = kwargs["tags"]
        fields = kwargs["fields"]
        timestamp = kwargs["timestamp"]
        day = timestamp.astimezone(pytz.timezone("Europe/Berlin")).date()
        if tags.get("entity_id") == "stored_energy" and "end" in fields:
            self.actions.append(("simulation", tags["scenario"], day))
        elif "quality" in fields:
            self.actions.append(("quality_write", tags["entity_id"], day))


def test_daily_quality_runs_after_current_only_for_simulated_days(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(
        _quality_config(enabled=True, include_second_scenario=True)
    )
    handler = DailyQualityHandler()
    runner = runner_module.BatteryScenarioRunner(
        handler, config, date(2026, 1, 1)
    )

    assert runner.process(last_day=date(2026, 1, 2)) == 2

    quality_writes = [write for write in handler.writes if "quality" in write["fields"]]
    assert len(quality_writes) == 6
    assert {write["tags"]["scenario"] for write in quality_writes} == {"current_battery"}
    assert {write["tags"]["entity_id"] for write in quality_writes} == {
        "soc_pct",
        "grid_import",
        "grid_export",
    }
    assert all(set(write["fields"]) == {"quality", "signed_error"} for write in quality_writes)
    assert all("measured" not in write["fields"] for write in quality_writes)
    assert {
        write["tags"]["entity_id"]: write["tags"]["unit"]
        for write in quality_writes[:3]
    } == {"soc_pct": "%", "grid_import": "kWh", "grid_export": "kWh"}
    assert len(handler.quality_reads) == 4
    assert handler.actions == [
        ("simulation", "current_battery", date(2026, 1, 1)),
        ("quality_read", "battery_soc", date(2026, 1, 1)),
        ("quality_read", "grid_power", date(2026, 1, 1)),
        ("quality_write", "soc_pct", date(2026, 1, 1)),
        ("quality_write", "grid_import", date(2026, 1, 1)),
        ("quality_write", "grid_export", date(2026, 1, 1)),
        ("simulation", "another_battery", date(2026, 1, 1)),
        ("simulation", "current_battery", date(2026, 1, 2)),
        ("quality_read", "battery_soc", date(2026, 1, 2)),
        ("quality_read", "grid_power", date(2026, 1, 2)),
        ("quality_write", "soc_pct", date(2026, 1, 2)),
        ("quality_write", "grid_import", date(2026, 1, 2)),
        ("quality_write", "grid_export", date(2026, 1, 2)),
        ("simulation", "another_battery", date(2026, 1, 2)),
    ]


def test_daily_quality_disabled_does_not_read_or_write_quality(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(_quality_config(enabled=False))
    handler = DailyQualityHandler()

    assert runner_module.BatteryScenarioRunner(
        handler, config, date(2026, 1, 1)
    ).process(last_day=date(2026, 1, 1)) == 1

    assert handler.quality_reads == []
    assert not [write for write in handler.writes if "quality" in write["fields"]]


def test_daily_quality_bootstraps_missing_midnight_state_but_keeps_simulation(
    fake_influx_module, caplog
):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(_quality_config(enabled=True))
    handler = DailyQualityHandler(missing_soc_start=True)

    assert runner_module.BatteryScenarioRunner(
        handler, config, date(2026, 1, 1)
    ).process(last_day=date(2026, 1, 1)) == 1

    assert any(
        write["tags"].get("entity_id") == "stored_energy" and "end" in write["fields"]
        for write in handler.writes
    )
    quality_writes = [write for write in handler.writes if "quality" in write["fields"]]
    assert len(quality_writes) == 3
    assert any(
        "battery_soc" in record.getMessage()
        and "Bootstrapping daily quality source" in record.getMessage()
        for record in caplog.records
    )
