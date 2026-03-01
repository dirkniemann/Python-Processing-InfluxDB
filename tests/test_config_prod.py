
import json
from pathlib import Path


def test_prod_config_structure():
    cfg_path = Path(__file__).parent.parent / "config" / "prod.json"
    with cfg_path.open(encoding="utf-8") as f:
        cfg = json.load(f)

    assert "processing" in cfg
    processing = cfg["processing"]
    assert processing["input_bucket"]
    assert processing["output_bucket"]
    daily = processing["entities_to_process"]["daily_aggregate"]
    assert daily["version"]
    assert isinstance(daily["entities"], list)
    assert all(isinstance(e, str) for e in daily["entities"])

    scenarios = cfg["scenarios"]
    assert "setup" in scenarios
    setup = scenarios["setup"]
    assert setup["bucket"]
    assert setup["base_storage_capacity_kWh"] > 0
    for name, scenario in scenarios.items():
        if name == "setup":
            continue
        assert scenario["description"]
        assert scenario["extra_storage_capacity_kWh"] >= 0
