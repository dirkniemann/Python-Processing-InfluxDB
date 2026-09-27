from datetime import datetime
import importlib

import pytz
import pytest


def test_validation_returns_plausible_ranges_without_calibrating_model():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    charge_energy = 2.0
    soc_delta = charge_energy / 22.4 * 100
    records_power = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": -2.0},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 2.0},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 2.0},
    ]
    records_soc = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": 50.0},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 50.0 + soc_delta},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 50.0},
    ]

    report = module.analyze_real_battery(records_power, records_soc, 22.4)

    assert report.status == "plausible_range"
    assert report.charge_efficiency == pytest.approx(1.0)
    assert report.discharge_efficiency == pytest.approx(1.0)


def test_validation_does_not_invent_efficiency_for_mismatched_capacity():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    records_power = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": -2.0},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 2.0},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 2.0},
    ]
    records_soc = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": 50.0},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 58.928571},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 50.0},
    ]

    report = module.analyze_real_battery(records_power, records_soc, 10.0)

    assert report.status == "indeterminate"
    assert report.charge_efficiency is None
    assert report.discharge_efficiency is None


def test_simulation_comparison_reports_soc_power_and_energy_differences():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    simulation = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "soc_pct": 50, "battery_charge_dc_kw": 1, "battery_discharge_dc_kw": 0, "run_version": "run"},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "soc_pct": 60, "battery_charge_dc_kw": 0, "battery_discharge_dc_kw": 1, "run_version": "run"},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "soc_pct": 50, "battery_charge_dc_kw": 0, "battery_discharge_dc_kw": 1, "run_version": "run"},
    ]
    power = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": -1},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 1},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 1},
    ]
    soc = [
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "value": 50},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "value": 60},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "value": 50},
    ]

    report = module.compare_simulation_to_real(simulation, power, soc)

    assert report.status == "available"
    assert report.sample_count == 3
    assert report.soc_mae_pct == 0
    assert report.measured_charge_energy_kwh == 1
    assert report.simulated_charge_energy_kwh == 1
