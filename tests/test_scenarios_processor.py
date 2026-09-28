from datetime import date, datetime
import importlib

import pytz


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
    assert timeseries["tags"]["unit"] == "kW"
    assert timeseries["fields"] == {"actual": 1.0}
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
