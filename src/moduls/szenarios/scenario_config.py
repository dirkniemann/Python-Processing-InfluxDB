from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class BatterySetup:
    base_capacity_kwh: float
    base_charge_power_kw: float
    base_discharge_power_kw: float
    min_soc_pct: float
    max_soc_pct: float
    initial_soc_pct: float
    loss_model: str
    charge_efficiency: float
    discharge_efficiency: float


@dataclass(frozen=True)
class ScenarioDefinition:
    name: str
    extra_capacity_kwh: float
    extra_power_kw: float
    battery_enabled: bool = True

    @property
    def capacity_kwh(self) -> float:
        if not self.battery_enabled:
            return 0.0
        return self._base_capacity_kwh + self.extra_capacity_kwh

    @property
    def charge_power_kw(self) -> float:
        if not self.battery_enabled:
            return 0.0
        return self._base_charge_power_kw + self.extra_power_kw

    @property
    def discharge_power_kw(self) -> float:
        if not self.battery_enabled:
            return 0.0
        return self._base_discharge_power_kw + self.extra_power_kw

    def with_base(self, setup: BatterySetup) -> "ScenarioDefinition":
        return ScenarioDefinitionWithSetup(
            name=self.name,
            extra_capacity_kwh=self.extra_capacity_kwh,
            extra_power_kw=self.extra_power_kw,
            battery_enabled=self.battery_enabled,
            _base_capacity_kwh=setup.base_capacity_kwh,
            _base_charge_power_kw=setup.base_charge_power_kw,
            _base_discharge_power_kw=setup.base_discharge_power_kw,
        )


@dataclass(frozen=True)
class ScenarioDefinitionWithSetup(ScenarioDefinition):
    _base_capacity_kwh: float = 0.0
    _base_charge_power_kw: float = 0.0
    _base_discharge_power_kw: float = 0.0


@dataclass(frozen=True)
class ScenarioSource:
    name: str
    bucket: str
    measurement: str
    entity_id: str
    field: str
    version: str | None = None
    change_only: bool = False
    allow_negative: bool = False
    negative_as_pv: bool = False


@dataclass(frozen=True)
class ScenarioConfiguration:
    version: str
    setup: BatterySetup
    buckets: Dict[str, str]
    sources: Dict[str, ScenarioSource]
    pv_sources: Dict[str, List[str]]
    pv_modes: Dict[str, List[str]]
    definitions: Dict[str, ScenarioDefinitionWithSetup]
    validation_sources: Dict[str, ScenarioSource]
    daily_quality_enabled: bool
    quality_grid_power_source: Optional[ScenarioSource]
    efficiency_enabled: bool


def load_scenario_configuration(config: Dict[str, Any]) -> ScenarioConfiguration:
    scenarios = config.get("scenarios")
    if not isinstance(scenarios, dict):
        raise ValueError("'scenarios' must be a dictionary")

    version = _require_string(scenarios, "version", "scenarios.version")
    buckets = _require_dict(scenarios, "buckets")
    for key in ("source_bucket", "processing_bucket", "output_bucket"):
        _require_string(buckets, key, f"scenarios.buckets.{key}")

    setup_values = _require_dict(scenarios, "setup")
    setup = BatterySetup(
        base_capacity_kwh=_positive_number(setup_values, "base_capacity_kwh"),
        base_charge_power_kw=_positive_number(setup_values, "base_charge_power_dc_kw"),
        base_discharge_power_kw=_positive_number(setup_values, "base_discharge_power_dc_kw"),
        min_soc_pct=_number(setup_values, "min_soc_pct"),
        max_soc_pct=_number(setup_values, "max_soc_pct"),
        initial_soc_pct=_number(setup_values, "initial_soc_pct"),
        loss_model=_require_string(setup_values, "loss_model", "scenarios.setup.loss_model"),
        charge_efficiency=_efficiency_number(setup_values, "charge_efficiency"),
        discharge_efficiency=_efficiency_number(setup_values, "discharge_efficiency"),
    )
    if not 0 <= setup.min_soc_pct < setup.max_soc_pct <= 100:
        raise ValueError("SOC limits must satisfy 0 <= min < max <= 100")
    if not setup.min_soc_pct <= setup.initial_soc_pct <= setup.max_soc_pct:
        raise ValueError("initial_soc_pct must be inside the SOC limits")
    if setup.loss_model != "idealized_no_additional_losses_v1":
        raise ValueError(f"Unsupported loss_model: {setup.loss_model}")

    sources_config = _require_dict(scenarios, "sources")
    sources: Dict[str, ScenarioSource] = {}
    for name, source_value in sources_config.items():
        source = _require_dict(sources_config, name)
        bucket_ref = _require_string(source, "bucket_ref", f"scenarios.sources.{name}.bucket_ref")
        if bucket_ref not in buckets:
            raise ValueError(f"Unknown bucket reference: {bucket_ref}")
        change_only = source.get("change_only", False)
        if not isinstance(change_only, bool):
            raise ValueError(f"scenarios.sources.{name}.change_only must be boolean")
        source_version = source.get("version")
        if source_version is not None and (
            not isinstance(source_version, str) or not source_version.strip()
        ):
            raise ValueError(f"scenarios.sources.{name}.version must be a non-empty string or null")
        allow_negative = source.get("allow_negative", False)
        negative_as_pv = source.get("negative_as_pv", False)
        if not isinstance(allow_negative, bool):
            raise ValueError(f"scenarios.sources.{name}.allow_negative must be boolean")
        if not isinstance(negative_as_pv, bool):
            raise ValueError(f"scenarios.sources.{name}.negative_as_pv must be boolean")
        if negative_as_pv and not allow_negative:
            raise ValueError(
                f"scenarios.sources.{name}.negative_as_pv requires allow_negative"
            )
        sources[name] = ScenarioSource(
            name=name,
            bucket=buckets[bucket_ref],
            measurement=_require_string(source, "measurement", f"scenarios.sources.{name}.measurement"),
            entity_id=_require_string(source, "entity_id", f"scenarios.sources.{name}.entity_id"),
            field=_require_string(source, "field", f"scenarios.sources.{name}.field"),
            version=source_version,
            change_only=change_only,
            allow_negative=allow_negative,
            negative_as_pv=negative_as_pv,
        )

    pv_sources_config = _require_dict(scenarios, "pv_sources")
    pv_sources: Dict[str, List[str]] = {}
    for group_name, source_names in pv_sources_config.items():
        if not isinstance(source_names, list) or not source_names:
            raise ValueError(f"scenarios.pv_sources.{group_name} must be a non-empty list")
        if any(name not in sources for name in source_names):
            raise ValueError(f"Unknown source in pv_sources.{group_name}")
        pv_sources[group_name] = source_names

    pv_modes_config = _require_dict(scenarios, "pv_modes")
    pv_modes: Dict[str, List[str]] = {}
    for mode_name, groups in pv_modes_config.items():
        if not isinstance(groups, list) or not groups:
            raise ValueError(f"scenarios.pv_modes.{mode_name} must be a non-empty list")
        if any(group not in pv_sources for group in groups):
            raise ValueError(f"Unknown PV group in pv_modes.{mode_name}")
        pv_modes[mode_name] = groups

    definitions_config = _require_dict(scenarios, "definitions")
    definitions: Dict[str, ScenarioDefinitionWithSetup] = {}
    for name, definition_value in definitions_config.items():
        definition = _require_dict(definitions_config, name)
        if definition.get("enabled") is not True:
            continue
        extra_capacity = _nonnegative_number(definition, "extra_capacity_kwh")
        extra_power = _nonnegative_number(definition, "extra_power_kw")
        battery_enabled = definition.get("battery_enabled", True)
        if not isinstance(battery_enabled, bool):
            raise ValueError(f"scenarios.definitions.{name}.battery_enabled must be boolean")
        definitions[name] = ScenarioDefinitionWithSetup(
            name=name,
            extra_capacity_kwh=extra_capacity,
            extra_power_kw=extra_power,
            battery_enabled=battery_enabled,
            _base_capacity_kwh=setup.base_capacity_kwh,
            _base_charge_power_kw=setup.base_charge_power_kw,
            _base_discharge_power_kw=setup.base_discharge_power_kw,
        )
    validation_sources: Dict[str, ScenarioSource] = {}
    validation_config = scenarios.get("validation", {})
    if validation_config:
        if not isinstance(validation_config, dict):
            raise ValueError("scenarios.validation must be a dictionary")
        for name in ("battery_power", "battery_soc"):
            source = _require_dict(validation_config, name)
            bucket_ref = _require_string(source, "bucket_ref", f"scenarios.validation.{name}.bucket_ref")
            if bucket_ref not in buckets:
                raise ValueError(f"Unknown validation bucket reference: {bucket_ref}")
            change_only = source.get("change_only", True)
            if not isinstance(change_only, bool):
                raise ValueError(f"scenarios.validation.{name}.change_only must be boolean")
            validation_sources[name] = ScenarioSource(
                name=name,
                bucket=buckets[bucket_ref],
                measurement=_require_string(source, "measurement", f"scenarios.validation.{name}.measurement"),
                entity_id=_require_string(source, "entity_id", f"scenarios.validation.{name}.entity_id"),
                field=_require_string(source, "field", f"scenarios.validation.{name}.field"),
                version=source.get("version"),
                change_only=change_only,
            )
        if set(validation_sources) != {"battery_power", "battery_soc"}:
            raise ValueError("scenarios.validation must define battery_power and battery_soc")

    quality_config = scenarios.get("quality", {})
    if not isinstance(quality_config, dict):
        raise ValueError("scenarios.quality must be a dictionary")
    daily_quality_enabled = quality_config.get("enabled", False)
    if not isinstance(daily_quality_enabled, bool):
        raise ValueError("scenarios.quality.enabled must be boolean")
    quality_grid_power_source = None
    if daily_quality_enabled:
        if "battery_soc" not in validation_sources:
            raise ValueError(
                "scenarios.quality.enabled requires scenarios.validation.battery_soc"
            )
        grid_source = _require_dict(quality_config, "grid_power")
        bucket_ref = _require_string(
            grid_source, "bucket_ref", "scenarios.quality.grid_power.bucket_ref"
        )
        if bucket_ref not in buckets:
            raise ValueError(f"Unknown bucket reference: {bucket_ref}")
        change_only = grid_source.get("change_only", True)
        if not isinstance(change_only, bool):
            raise ValueError("scenarios.quality.grid_power.change_only must be boolean")
        quality_grid_power_source = ScenarioSource(
            name="grid_power",
            bucket=buckets[bucket_ref],
            measurement=_require_string(
                grid_source, "measurement", "scenarios.quality.grid_power.measurement"
            ),
            entity_id=_require_string(
                grid_source, "entity_id", "scenarios.quality.grid_power.entity_id"
            ),
            field=_require_string(
                grid_source, "field", "scenarios.quality.grid_power.field"
            ),
            version=grid_source.get("version"),
            change_only=change_only,
        )

    efficiency_config = scenarios.get("efficiency", {})
    if not isinstance(efficiency_config, dict):
        raise ValueError("scenarios.efficiency must be a dictionary")
    efficiency_enabled = efficiency_config.get("enabled", False)
    if not isinstance(efficiency_enabled, bool):
        raise ValueError("scenarios.efficiency.enabled must be boolean")
    if efficiency_enabled and set(validation_sources) != {"battery_power", "battery_soc"}:
        raise ValueError(
            "scenarios.efficiency.enabled requires battery_power and battery_soc validation sources"
        )

    return ScenarioConfiguration(
        version=version,
        setup=setup,
        buckets={key: value for key, value in buckets.items()},
        sources=sources,
        pv_sources=pv_sources,
        pv_modes=pv_modes,
        definitions=definitions,
        validation_sources=validation_sources,
        daily_quality_enabled=daily_quality_enabled,
        quality_grid_power_source=quality_grid_power_source,
        efficiency_enabled=efficiency_enabled,
    )


def _require_dict(parent: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a dictionary")
    return value


def _require_string(parent: Dict[str, Any], key: str, label: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _number(parent: Dict[str, Any], key: str) -> float:
    value = parent.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    return float(value)


def _positive_number(parent: Dict[str, Any], key: str) -> float:
    value = _number(parent, key)
    if value <= 0:
        raise ValueError(f"{key} must be positive")
    return value


def _nonnegative_number(parent: Dict[str, Any], key: str) -> float:
    value = _number(parent, key)
    if value < 0:
        raise ValueError(f"{key} must be non-negative")
    return value


def _efficiency_number(parent: Dict[str, Any], key: str) -> float:
    value = parent.get(key, 1.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    value = float(value)
    if not 0 < value <= 1:
        raise ValueError(f"{key} must be greater than 0 and at most 1")
    return value
