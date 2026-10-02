from dataclasses import dataclass
from datetime import datetime

from moduls.szenarios.scenario_config import ScenarioDefinitionWithSetup


@dataclass
class BatteryState:
    stored_energy_kwh: float


@dataclass(frozen=True)
class IntervalResult:
    timestamp: datetime
    duration_s: float
    soc_pct: float
    stored_energy_kwh: float
    house_load_kw: float
    pv_generation_kw: float
    pv_to_load_kw: float
    pv_to_battery_kw: float
    battery_to_load_kw: float
    battery_charge_dc_kw: float
    battery_discharge_dc_kw: float
    grid_import_kw: float
    grid_export_kw: float
    pv_export_kw: float


class BatteryScenarioEngine:
    """Idealized V1 self-consumption battery model."""

    def __init__(
        self,
        definition: ScenarioDefinitionWithSetup,
        min_soc_pct: float,
        max_soc_pct: float,
        charge_efficiency: float = 1.0,
        discharge_efficiency: float = 1.0,
    ):
        self.definition = definition
        self.min_soc_pct = min_soc_pct
        self.max_soc_pct = max_soc_pct
        if not 0 < charge_efficiency <= 1 or not 0 < discharge_efficiency <= 1:
            raise ValueError("battery efficiencies must be greater than 0 and at most 1")
        self.charge_efficiency = charge_efficiency
        self.discharge_efficiency = discharge_efficiency
        self.min_energy_kwh = definition.capacity_kwh * min_soc_pct / 100
        self.max_energy_kwh = definition.capacity_kwh * max_soc_pct / 100

    def initial_state(self, initial_soc_pct: float) -> BatteryState:
        return BatteryState(self.definition.capacity_kwh * initial_soc_pct / 100)

    def simulate_interval(
        self,
        state: BatteryState,
        timestamp: datetime,
        duration_s: float,
        house_load_kw: float,
        pv_generation_kw: float,
    ) -> IntervalResult:
        if duration_s <= 0:
            raise ValueError("duration_s must be positive")
        if house_load_kw < 0 or pv_generation_kw < 0:
            raise ValueError("house load and PV generation must be non-negative")
        if state.stored_energy_kwh < self.min_energy_kwh - 1e-9:
            raise ValueError("battery state is below the minimum SOC")
        if state.stored_energy_kwh > self.max_energy_kwh + 1e-9:
            raise ValueError("battery state is above the maximum SOC")

        direct_pv_kw = min(house_load_kw, pv_generation_kw)
        surplus_kw = max(0.0, pv_generation_kw - direct_pv_kw)
        deficit_kw = max(0.0, house_load_kw - direct_pv_kw)
        interval_hours = duration_s / 3600

        charge_limit_kw = min(
            surplus_kw,
            self.definition.charge_power_kw,
            max(0.0, self.max_energy_kwh - state.stored_energy_kwh)
            / self.charge_efficiency
            / interval_hours,
        )
        state.stored_energy_kwh += (
            charge_limit_kw * interval_hours * self.charge_efficiency
        )
        discharge_limit_kw = min(
            deficit_kw,
            self.definition.discharge_power_kw,
            max(0.0, state.stored_energy_kwh - self.min_energy_kwh)
            * self.discharge_efficiency
            / interval_hours,
        )
        state.stored_energy_kwh -= (
            discharge_limit_kw * interval_hours / self.discharge_efficiency
        )
        state.stored_energy_kwh = min(
            self.max_energy_kwh,
            max(self.min_energy_kwh, state.stored_energy_kwh),
        )

        pv_export_kw = max(0.0, surplus_kw - charge_limit_kw)
        grid_import_kw = max(0.0, deficit_kw - discharge_limit_kw)
        soc_pct = state.stored_energy_kwh / self.definition.capacity_kwh * 100
        return IntervalResult(
            timestamp=timestamp,
            duration_s=duration_s,
            soc_pct=soc_pct,
            stored_energy_kwh=state.stored_energy_kwh,
            house_load_kw=house_load_kw,
            pv_generation_kw=pv_generation_kw,
            pv_to_load_kw=direct_pv_kw,
            pv_to_battery_kw=charge_limit_kw,
            battery_to_load_kw=discharge_limit_kw,
            battery_charge_dc_kw=charge_limit_kw,
            battery_discharge_dc_kw=discharge_limit_kw,
            grid_import_kw=grid_import_kw,
            grid_export_kw=pv_export_kw,
            pv_export_kw=pv_export_kw,
        )
