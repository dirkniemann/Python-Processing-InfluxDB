
import json
import importlib
import copy
from pathlib import Path


def test_prod_config_structure():
    cfg_path = Path(__file__).parent.parent / "config" / "prod.json"
    with cfg_path.open(encoding="utf-8") as f:
        cfg = json.load(f)

    assert "processing" in cfg
    mqtt = cfg["mqtt"]
    assert mqtt["enabled"] is True
    assert mqtt["host"]
    assert 1 <= mqtt["port"] <= 65535
    assert mqtt["discovery_prefix"] == "homeassistant"
    assert "password" not in mqtt
    assert "token" not in mqtt
    processing = cfg["processing"]
    assert processing["input_bucket"]
    assert processing["output_bucket"]
    daily = processing["entities_to_process"]["daily_aggregate"]
    assert daily["version"]
    assert isinstance(daily["entities"], list)
    assert all(isinstance(e, str) for e in daily["entities"])

    scenarios = cfg["scenarios"]
    assert scenarios["buckets"]["output_bucket"]
    assert "setup" in scenarios
    setup = scenarios["setup"]
    assert setup["base_capacity_kwh"] > 0
    assert setup["min_soc_pct"] < setup["max_soc_pct"]
    assert setup["initial_soc_pct"] == setup["min_soc_pct"]
    assert scenarios["pv_modes"]["without_old_pv"]
    assert scenarios["pv_modes"]["with_old_pv"]
    for name, scenario in scenarios["definitions"].items():
        assert isinstance(scenario["enabled"], bool)
        assert scenario["extra_capacity_kwh"] >= 0
        assert scenario["extra_power_kw"] >= 0


def test_prod_scenario_loader_keeps_scenarios_and_modes_configurable(prod_config):
    module = importlib.import_module("moduls.szenarios.scenario_config")
    active_config = copy.deepcopy(prod_config)
    for definition in active_config["scenarios"]["definitions"].values():
        definition["enabled"] = True
    configuration = module.load_scenario_configuration(active_config)

    assert set(configuration.definitions) == {
        "current_battery",
        "8_modules_2_towers",
        "7_modules_1_tower",
        "14_modules_2_towers",
    }
    assert set(configuration.pv_modes) == {"without_old_pv", "with_old_pv"}
    assert configuration.definitions["current_battery"].capacity_kwh == 22.4
    assert configuration.definitions["7_modules_1_tower"].charge_power_kw == 30.0
    assert set(configuration.validation_sources) == {"battery_power", "battery_soc"}


def test_all_scenarios_can_be_disabled_without_invalidating_config(prod_config):
    module = importlib.import_module("moduls.szenarios.scenario_config")
    disabled_config = copy.deepcopy(prod_config)
    for definition in disabled_config["scenarios"]["definitions"].values():
        definition["enabled"] = False

    configuration = module.load_scenario_configuration(disabled_config)

    assert configuration.definitions == {}


def test_daily_quality_configuration_is_explicit_and_resolves_grid_source(prod_config):
    module = importlib.import_module("moduls.szenarios.scenario_config")
    configuration = module.load_scenario_configuration(prod_config)

    assert configuration.daily_quality_enabled is True
    assert configuration.quality_grid_power_source is not None
    assert configuration.quality_grid_power_source.bucket == "HomeAssistant"
    assert configuration.quality_grid_power_source.entity_id == "fems_gridactivepower"


def test_dev_daily_quality_uses_real_soc_measurement_name():
    config_path = Path(__file__).parent.parent / "config" / "dev.json"
    with config_path.open(encoding="utf-8") as config_file:
        config_data = json.load(config_file)

    module = importlib.import_module("moduls.szenarios.scenario_config")
    configuration = module.load_scenario_configuration(config_data)

    assert configuration.daily_quality_enabled is True
    assert configuration.validation_sources["battery_soc"].measurement == "„%“"
    assert configuration.quality_grid_power_source is not None
    assert configuration.quality_grid_power_source.entity_id == "fems_gridactivepower"
