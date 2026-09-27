from datetime import datetime
import importlib

import pytz
import pytest


def make_engine():
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    engine_module = importlib.import_module("moduls.szenarios.battery_engine")
    definition = config_module.ScenarioDefinitionWithSetup(
        name="dst",
        extra_capacity_kwh=0,
        extra_power_kw=0,
        _base_capacity_kwh=10,
        _base_charge_power_kw=5,
        _base_discharge_power_kw=5,
    )
    return engine_module.BatteryScenarioEngine(definition, 0, 100)


def test_normal_day_integrates_24_hours():
    handler_module = importlib.import_module("moduls.influxdb_handler")
    engine = make_engine()
    result = engine.simulate_interval(
        engine.initial_state(50),
        handler_module.local_to_utc(datetime(2026, 1, 10)),
        24 * 3600,
        house_load_kw=0.1,
        pv_generation_kw=0,
    )
    assert result.stored_energy_kwh == pytest.approx(2.6)


def test_spring_dst_day_integrates_23_hours():
    handler_module = importlib.import_module("moduls.influxdb_handler")
    engine = make_engine()
    start = handler_module.local_to_utc(datetime(2026, 3, 29))
    end = handler_module.local_to_utc(datetime(2026, 3, 30))
    result = engine.simulate_interval(
        engine.initial_state(50), start, (end - start).total_seconds(), 0.1, 0
    )
    assert (end - start).total_seconds() == 23 * 3600
    assert result.stored_energy_kwh == pytest.approx(2.7)


def test_autumn_dst_day_integrates_25_hours():
    handler_module = importlib.import_module("moduls.influxdb_handler")
    engine = make_engine()
    start = handler_module.local_to_utc(datetime(2026, 10, 25))
    end = handler_module.local_to_utc(datetime(2026, 10, 26))
    result = engine.simulate_interval(
        engine.initial_state(50), start, (end - start).total_seconds(), 0.1, 0
    )
    assert (end - start).total_seconds() == 25 * 3600
    assert result.stored_energy_kwh == pytest.approx(2.5)
