from datetime import datetime
import importlib

import pytest


def make_engine(charge_efficiency=1.0, discharge_efficiency=1.0):
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
    return module.BatteryScenarioEngine(
        definition,
        min_soc_pct=0,
        max_soc_pct=100,
        charge_efficiency=charge_efficiency,
        discharge_efficiency=discharge_efficiency,
    )


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


def test_charge_efficiency_reduces_stored_energy():
    engine = make_engine(charge_efficiency=0.8)
    state = engine.initial_state(0)

    result = engine.simulate_interval(
        state, datetime(2026, 1, 1), 3600, house_load_kw=0, pv_generation_kw=5
    )

    assert result.pv_to_battery_kw == pytest.approx(5)
    assert result.stored_energy_kwh == pytest.approx(4)
    assert result.pv_export_kw == pytest.approx(0)


def test_discharge_efficiency_increases_removed_energy():
    engine = make_engine(discharge_efficiency=0.8)
    state = engine.initial_state(50)

    result = engine.simulate_interval(
        state, datetime(2026, 1, 1), 3600, house_load_kw=10, pv_generation_kw=0
    )

    assert result.battery_to_load_kw == pytest.approx(4)
    assert result.grid_import_kw == pytest.approx(6)
    assert result.stored_energy_kwh == pytest.approx(0)


def test_negative_inputs_are_rejected():
    engine = make_engine()
    with pytest.raises(ValueError, match="non-negative"):
        engine.simulate_interval(
            engine.initial_state(50), datetime(2026, 1, 1), 3600, -1, 0
        )


def test_zero_capacity_reference_has_no_battery_flows():
    module = importlib.import_module("moduls.szenarios.battery_engine")
    config_module = importlib.import_module("moduls.szenarios.scenario_config")
    definition = config_module.ScenarioDefinitionWithSetup(
        name="without_battery",
        extra_capacity_kwh=0,
        extra_power_kw=0,
        battery_enabled=False,
        _base_capacity_kwh=10,
        _base_charge_power_kw=5,
        _base_discharge_power_kw=5,
    )
    engine = module.BatteryScenarioEngine(definition, min_soc_pct=5, max_soc_pct=100)

    result = engine.simulate_interval(
        engine.initial_state(5),
        datetime(2026, 1, 1),
        3600,
        house_load_kw=2,
        pv_generation_kw=10,
    )

    assert definition.capacity_kwh == 0
    assert result.soc_pct == 0
    assert result.pv_to_battery_kw == 0
    assert result.battery_to_load_kw == 0
    assert result.grid_import_kw == 0
    assert result.grid_export_kw == 8
