from datetime import datetime
import importlib

import pytest


def make_engine():
    module = importlib.import_module("moduls.szenarios.battery_engine")
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    definition = config_module.ScenarioDefinitionWithSetup(
        name="current_battery",
        extra_capacity_kwh=0,
        extra_power_kw=0,
        _base_capacity_kwh=10,
        _base_charge_power_kw=5,
        _base_discharge_power_kw=5,
    )
    return module.BatteryScenarioEngine(definition, min_soc_pct=0, max_soc_pct=100)


def test_surplus_pv_charges_then_exports():
    engine = make_engine()
    state = engine.initial_state(50)

    result = engine.simulate_interval(
        state, datetime(2026, 1, 1), 3600, house_load_kw=2, pv_generation_kw=10
    )

    assert result.pv_to_load_kw == pytest.approx(2)
    assert result.pv_to_battery_kw == pytest.approx(5)
    assert result.pv_export_kw == pytest.approx(3)
    assert result.grid_import_kw == pytest.approx(0)
    assert result.stored_energy_kwh == pytest.approx(10)


def test_deficit_uses_battery_then_imports():
    engine = make_engine()
    state = engine.initial_state(50)

    result = engine.simulate_interval(
        state, datetime(2026, 1, 1), 3600, house_load_kw=10, pv_generation_kw=2
    )

    assert result.battery_to_load_kw == pytest.approx(5)
    assert result.grid_import_kw == pytest.approx(3)
    assert result.stored_energy_kwh == pytest.approx(0)


def test_soc_limit_prevents_overcharge():
    engine = make_engine()
    state = engine.initial_state(95)

    result = engine.simulate_interval(
        state, datetime(2026, 1, 1), 3600, house_load_kw=0, pv_generation_kw=10
    )

    assert result.pv_to_battery_kw == pytest.approx(0.5)
    assert result.pv_export_kw == pytest.approx(9.5)
    assert result.soc_pct == pytest.approx(100)


def test_negative_inputs_are_rejected():
    engine = make_engine()
    with pytest.raises(ValueError, match="non-negative"):
        engine.simulate_interval(
            engine.initial_state(50), datetime(2026, 1, 1), 3600, -1, 0
        )
