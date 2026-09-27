from datetime import date, datetime
import importlib

import pytz


def minimal_config():
    return {
        "scenarios": {
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
    assert measurements.count("battery_scenario_timeseries") == 1
    assert measurements.count("battery_scenario_daily") == 1
    timeseries = next(write for write in handler.writes if write["measurement"] == "battery_scenario_timeseries")
    assert timeseries["tags"]["scenario"] == "current_battery"
    assert timeseries["tags"]["pv_mode"] == "without_old_pv"
    assert timeseries["fields"]["pv_to_load_kw"] == 1.0
    daily = next(write for write in handler.writes if write["measurement"] == "battery_scenario_daily")
    assert daily["fields"]["pv_to_load_kwh"] == 24.0
    assert daily["fields"]["pv_export_kwh"] == 43.0


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
            if kwargs["measurement"] == "battery_scenario_daily":
                self.daily.append({"time": kwargs["timestamp"], **kwargs["fields"], **kwargs["tags"]})

    handler = Handler()
    first_runner = runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1))
    assert first_runner.process(last_day=date(2026, 1, 1)) == 1
    first_run = handler.daily[0]["run_version"]

    restarted = runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1))
    assert restarted.process(last_day=date(2026, 1, 2)) == 1
    assert len(handler.daily) == 2
    assert handler.daily[1]["local_day"] == "2026-01-02"
    assert handler.daily[1]["run_version"] == first_run
    assert handler.daily[1]["soc_start_pct"] == handler.daily[0]["soc_end_pct"]


def test_runner_starts_new_run_after_historical_input_change(fake_influx_module):
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(minimal_config())

    class Handler:
        def __init__(self):
            self.daily = []
            self.changed = False

        def get_scenario_daily_records(self, **kwargs):
            return list(self.daily)

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": kwargs["stop_time"], "value": 1000}

        def get_data(self, **kwargs):
            value = 2000 if self.changed and kwargs["entity_id"] == "Hausverbrauch_korrigiert" else 1000
            return [{"time": kwargs["start_time"], "value": value}]

        def write_fields_datapoint(self, **kwargs):
            if kwargs["measurement"] == "battery_scenario_daily":
                self.daily.append({"time": kwargs["timestamp"], **kwargs["fields"], **kwargs["tags"]})

    handler = Handler()
    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(last_day=date(2026, 1, 2))
    old_version = handler.daily[0]["run_version"]
    handler.changed = True

    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(last_day=date(2026, 1, 2))
    new_records = handler.daily[2:]
    assert new_records
    assert all(record["run_version"] != old_version for record in new_records)
    assert all(record["run_reason"] == "historical_input_change" for record in new_records)


def test_runner_marks_missing_non_change_only_source_incomplete(fake_influx_module):
    config_data = minimal_config()
    config_data["scenarios"]["sources"]["fems_pv"]["change_only"] = False
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    runner_module = importlib.import_module("moduls.szenarios.scenarios_processor")
    config = config_module.load_scenario_configuration(config_data)

    class Handler:
        def get_scenario_daily_records(self, **kwargs):
            return []

        def get_latest_datapoint_by_time(self, **kwargs):
            return {"time": kwargs["stop_time"], "value": 1000} if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else None

        def get_data(self, **kwargs):
            return [{"time": kwargs["start_time"], "value": 1000}] if kwargs["entity_id"] == "Hausverbrauch_korrigiert" else []

        def write_fields_datapoint(self, **kwargs):
            self.write = kwargs

    handler = Handler()
    runner_module.BatteryScenarioRunner(handler, config, date(2026, 1, 1)).process(last_day=date(2026, 1, 1))
    assert handler.write["measurement"] == "battery_scenario_daily"
    assert handler.write["fields"]["is_complete"] is False
    assert "missing_initial_state:fems_pv" in handler.write["fields"]["quality_reason_code"]
