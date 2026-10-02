from datetime import datetime, timedelta
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
        {"time": utc.localize(datetime(2026, 1, 1, 0)), "soc_pct": 50, "battery_charge_dc_kw": 1, "battery_discharge_dc_kw": 0},
        {"time": utc.localize(datetime(2026, 1, 1, 1)), "soc_pct": 60, "battery_charge_dc_kw": 0, "battery_discharge_dc_kw": 1},
        {"time": utc.localize(datetime(2026, 1, 1, 2)), "soc_pct": 50, "battery_charge_dc_kw": 0, "battery_discharge_dc_kw": 1},
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


def test_daily_quality_uses_prior_state_and_reports_absolute_and_signed_errors():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    start = utc.localize(datetime(2026, 1, 1, 0))
    stop = start + timedelta(hours=3)
    simulated_soc = [
        {"time": start, "value": 60},
        {"time": start + timedelta(hours=2), "value": 40},
    ]
    measured_soc = [
        {"time": start - timedelta(hours=1), "value": 50},
        {"time": start + timedelta(hours=1), "value": 70},
    ]
    grid_power = [
        {"time": start - timedelta(hours=1), "value": 2000},
        {"time": start + timedelta(hours=1), "value": -1000},
        {"time": start + timedelta(hours=2), "value": 500},
    ]

    report = module.calculate_daily_simulation_quality(
        simulated_soc_records=simulated_soc,
        measured_soc_records=measured_soc,
        measured_grid_power_records=grid_power,
        simulated_grid_import_kwh=3.5,
        simulated_grid_export_kwh=0.25,
        start=start,
        stop=stop,
    )

    assert report is not None
    assert report.soc_mae_pct == pytest.approx(10)
    assert report.soc_signed_error_pct == pytest.approx(0)
    assert report.grid_import_quality_kwh == pytest.approx(1)
    assert report.grid_import_signed_error_kwh == pytest.approx(1)
    assert report.grid_export_quality_kwh == pytest.approx(0.75)
    assert report.grid_export_signed_error_kwh == pytest.approx(-0.75)


def test_daily_battery_efficiency_uses_directional_energy_and_soc():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    start = utc.localize(datetime(2026, 1, 1, 0))
    stop = start + timedelta(hours=3)
    result = module.calculate_daily_battery_efficiency(
        power_records=[
            {"time": start, "value": -2000},
            {"time": start + timedelta(minutes=15), "value": 2000},
            {"time": start + timedelta(minutes=30), "value": 0},
        ],
        soc_records=[
            {"time": start, "value": 50},
            {"time": start + timedelta(minutes=15), "value": 52.2321428571},
            {"time": start + timedelta(minutes=30), "value": 50},
        ],
        capacity_kwh=22.4,
        start=start,
        stop=stop,
    )

    assert result.charge_efficiency == pytest.approx(1.0)
    assert result.discharge_efficiency == pytest.approx(1.0)


def test_daily_quality_bootstraps_missing_start_state_from_first_valid_sample():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    start = utc.localize(datetime(2026, 1, 1, 0))
    stop = start + timedelta(hours=1)

    report = module.calculate_daily_simulation_quality(
        simulated_soc_records=[{"time": start, "value": 50}],
        measured_soc_records=[
            {"time": start + timedelta(minutes=1), "value": 50}
        ],
        measured_grid_power_records=[{"time": start, "value": 0}],
        simulated_grid_import_kwh=0,
        simulated_grid_export_kwh=0,
        start=start,
        stop=stop,
    )

    assert report is not None
    assert report.soc_mae_pct == 0


def test_timeseries_errors_are_signed_against_held_measurements():
    module = importlib.import_module("moduls.szenarios.battery_validation")
    utc = pytz.UTC
    start = utc.localize(datetime(2026, 1, 1, 0))
    simulated = [
        {
            "time": start,
            "soc_pct": 60,
            "grid_import_w": 1200,
            "grid_export_w": 0,
        },
        {
            "time": start + timedelta(hours=1),
            "soc_pct": 40,
            "grid_import_w": 0,
            "grid_export_w": 500,
        },
    ]

    errors = module.calculate_timeseries_simulation_errors(
        simulated_records=simulated,
        measured_soc_records=[
            {"time": start - timedelta(minutes=1), "value": 50},
        ],
        measured_grid_power_records=[
            {"time": start - timedelta(minutes=1), "value": 1000},
        ],
    )

    assert errors == [
        {
            "time": start,
            "soc_pct": 10,
            "grid_import_w": 200,
            "grid_export_w": 0,
        },
        {
            "time": start + timedelta(hours=1),
            "soc_pct": -10,
            "grid_import_w": -1000,
            "grid_export_w": 500,
        },
    ]
