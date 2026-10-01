import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class BatteryValidationReport:
    status: str
    reason: str
    sample_count: int
    charge_energy_kwh: float
    discharge_energy_kwh: float
    soc_charge_energy_kwh: float
    soc_discharge_energy_kwh: float
    charge_efficiency: Optional[float]
    discharge_efficiency: Optional[float]


@dataclass(frozen=True)
class SimulationComparisonReport:
    status: str
    reason: str
    sample_count: int
    soc_mae_pct: Optional[float]
    power_mae_kw: Optional[float]
    measured_charge_energy_kwh: Optional[float]
    simulated_charge_energy_kwh: Optional[float]
    measured_discharge_energy_kwh: Optional[float]
    simulated_discharge_energy_kwh: Optional[float]


@dataclass(frozen=True)
class DailySimulationQuality:
    """Daily current-battery errors against measured SOC and grid power."""

    soc_mae_pct: float
    soc_signed_error_pct: float
    grid_import_quality_kwh: float
    grid_import_signed_error_kwh: float
    grid_export_quality_kwh: float
    grid_export_signed_error_kwh: float


def calculate_daily_simulation_quality(
    simulated_soc_records: List[Dict[str, Any]],
    measured_soc_records: List[Dict[str, Any]],
    measured_grid_power_records: List[Dict[str, Any]],
    simulated_grid_import_kwh: float,
    simulated_grid_export_kwh: float,
    start: datetime,
    stop: datetime,
) -> Optional[DailySimulationQuality]:
    """Calculate one day's time-weighted SOC error and import/export errors.

    SOC and grid-power inputs use sample-and-hold semantics. The record lists
    must contain a value at or before ``start`` so the whole local day is
    covered. Grid power is in watts: positive means import, negative means
    export. The measured daily totals are used only for these calculations.
    """
    if start.tzinfo is None or stop.tzinfo is None or stop <= start:
        raise ValueError("quality window must be a positive timezone-aware interval")

    simulated_soc = _normalise(simulated_soc_records)
    measured_soc = _normalise(measured_soc_records)
    grid_power = _normalise(measured_grid_power_records)
    if not simulated_soc or not measured_soc or not grid_power:
        return None

    simulated_soc_series = _daily_series(simulated_soc, start, stop)
    measured_soc_series = _daily_series(measured_soc, start, stop)
    grid_series = _daily_series(grid_power, start, stop)
    if simulated_soc_series is None or measured_soc_series is None or grid_series is None:
        return None
    if any(not 0 <= value <= 100 for _, value in simulated_soc_series + measured_soc_series):
        return None

    duration_s = (stop - start).total_seconds()
    absolute_soc_error = signed_soc_error = 0.0
    timeline = sorted(
        {start, stop}
        | {timestamp for timestamp, _ in simulated_soc_series}
        | {timestamp for timestamp, _ in measured_soc_series}
    )
    simulated_index = measured_index = 0
    simulated_soc_value = measured_soc_value = None
    for timestamp, next_timestamp in zip(timeline, timeline[1:]):
        while (
            simulated_index < len(simulated_soc_series)
            and simulated_soc_series[simulated_index][0] <= timestamp
        ):
            simulated_soc_value = simulated_soc_series[simulated_index][1]
            simulated_index += 1
        while (
            measured_index < len(measured_soc_series)
            and measured_soc_series[measured_index][0] <= timestamp
        ):
            measured_soc_value = measured_soc_series[measured_index][1]
            measured_index += 1
        if simulated_soc_value is None or measured_soc_value is None:
            return None
        elapsed_s = (next_timestamp - timestamp).total_seconds()
        error = simulated_soc_value - measured_soc_value
        absolute_soc_error += abs(error) * elapsed_s
        signed_soc_error += error * elapsed_s

    measured_import_kwh, measured_export_kwh = _integrate_grid_power(grid_series, start, stop)
    import_signed_error = float(simulated_grid_import_kwh) - measured_import_kwh
    export_signed_error = float(simulated_grid_export_kwh) - measured_export_kwh
    return DailySimulationQuality(
        soc_mae_pct=absolute_soc_error / duration_s,
        soc_signed_error_pct=signed_soc_error / duration_s,
        grid_import_quality_kwh=abs(import_signed_error),
        grid_import_signed_error_kwh=import_signed_error,
        grid_export_quality_kwh=abs(export_signed_error),
        grid_export_signed_error_kwh=export_signed_error,
    )


def _daily_series(
    records: List[Dict[str, Any]], start: datetime, stop: datetime
) -> Optional[List[tuple[datetime, float]]]:
    """Keep the latest state at the window start and in-window changes."""
    previous = [record for record in records if record["time"] <= start]
    if not previous:
        return None
    start_record = previous[-1]
    by_time: Dict[datetime, float] = {start: start_record["value"]}
    for record in records:
        timestamp = record["time"]
        if start < timestamp < stop:
            by_time[timestamp] = record["value"]
        elif timestamp == start:
            by_time[start] = record["value"]
    return sorted(by_time.items())


def _integrate_grid_power(
    series: List[tuple[datetime, float]], start: datetime, stop: datetime
) -> tuple[float, float]:
    import_kwh = export_kwh = 0.0
    for index, (timestamp, value) in enumerate(series):
        next_timestamp = series[index + 1][0] if index + 1 < len(series) else stop
        interval_start = max(timestamp, start)
        interval_stop = min(next_timestamp, stop)
        if interval_stop <= interval_start:
            continue
        duration_hours = (interval_stop - interval_start).total_seconds() / 3600
        import_kwh += max(value, 0.0) / 1000 * duration_hours
        export_kwh += max(-value, 0.0) / 1000 * duration_hours
    return import_kwh, export_kwh


def analyze_real_battery(
    power_records: List[Dict[str, Any]],
    soc_records: List[Dict[str, Any]],
    capacity_kwh: float,
) -> BatteryValidationReport:
    """Compare measured battery power against SOC movement without calibrating V1.

    Positive battery power means discharge and negative power means charge. The
    function returns ``indeterminate`` when coverage, alignment, or ratios do
    not support a defensible empirical estimate.
    """
    power = _normalise(power_records)
    soc = _normalise(soc_records)
    if capacity_kwh <= 0 or not power or not soc:
        return _indeterminate("insufficient_power_or_soc_data")

    event_times = sorted({item["time"] for item in power + soc})
    current_power: Optional[float] = None
    current_soc: Optional[float] = None
    charge_energy = 0.0
    discharge_energy = 0.0
    soc_charge_energy = 0.0
    soc_discharge_energy = 0.0
    samples = 0
    power_index = 0
    soc_index = 0

    for index, timestamp in enumerate(event_times[:-1]):
        while power_index < len(power) and power[power_index]["time"] <= timestamp:
            current_power = power[power_index]["value"]
            power_index += 1
        while soc_index < len(soc) and soc[soc_index]["time"] <= timestamp:
            current_soc = soc[soc_index]["value"]
            soc_index += 1
        next_timestamp = event_times[index + 1]
        duration_hours = (next_timestamp - timestamp).total_seconds() / 3600
        if duration_hours <= 0 or current_power is None or current_soc is None:
            continue

        if current_power < 0:
            charge_energy += abs(current_power) * duration_hours
        elif current_power > 0:
            discharge_energy += current_power * duration_hours

        next_soc = current_soc
        if soc_index < len(soc) and soc[soc_index]["time"] <= next_timestamp:
            next_soc = soc[soc_index]["value"]
        delta_soc = next_soc - current_soc
        if delta_soc > 0:
            soc_charge_energy += delta_soc / 100 * capacity_kwh
        elif delta_soc < 0:
            soc_discharge_energy += abs(delta_soc) / 100 * capacity_kwh
        samples += 1

    if samples < 2 or charge_energy <= 0 or discharge_energy <= 0:
        return _indeterminate("insufficient_bidirectional_coverage", samples, charge_energy, discharge_energy, soc_charge_energy, soc_discharge_energy)

    charge_efficiency = soc_charge_energy / charge_energy
    discharge_efficiency = soc_discharge_energy / discharge_energy
    if not 0.75 <= charge_efficiency <= 1.25 or not 0.75 <= discharge_efficiency <= 1.25:
        return BatteryValidationReport(
            status="indeterminate",
            reason="dc_ac_soc_alignment_or_capacity_mismatch",
            sample_count=samples,
            charge_energy_kwh=charge_energy,
            discharge_energy_kwh=discharge_energy,
            soc_charge_energy_kwh=soc_charge_energy,
            soc_discharge_energy_kwh=soc_discharge_energy,
            charge_efficiency=None,
            discharge_efficiency=None,
        )

    return BatteryValidationReport(
        status="plausible_range",
        reason="aligned_bidirectional_measurements",
        sample_count=samples,
        charge_energy_kwh=charge_energy,
        discharge_energy_kwh=discharge_energy,
        soc_charge_energy_kwh=soc_charge_energy,
        soc_discharge_energy_kwh=soc_discharge_energy,
        charge_efficiency=charge_efficiency,
        discharge_efficiency=discharge_efficiency,
    )


def compare_simulation_to_real(
    simulated_records: List[Dict[str, Any]],
    power_records: List[Dict[str, Any]],
    soc_records: List[Dict[str, Any]],
) -> SimulationComparisonReport:
    """Compare the current-battery simulation against held real measurements."""
    simulation = sorted(
        [record for record in simulated_records if isinstance(record.get("time"), datetime)],
        key=lambda record: record["time"],
    )
    power = _normalise(power_records)
    soc = _normalise(soc_records)
    if len(simulation) < 2 or not power or not soc:
        return SimulationComparisonReport(
            "indeterminate", "insufficient_comparison_data", 0, None, None, None, None, None, None
        )

    soc_values = []
    power_errors = []
    measured_charge = measured_discharge = 0.0
    simulated_charge = simulated_discharge = 0.0
    power_index = soc_index = 0
    current_power = current_soc = None
    for index, record in enumerate(simulation):
        timestamp = record["time"]
        while power_index < len(power) and power[power_index]["time"] <= timestamp:
            current_power = power[power_index]["value"]
            power_index += 1
        while soc_index < len(soc) and soc[soc_index]["time"] <= timestamp:
            current_soc = soc[soc_index]["value"]
            soc_index += 1
        if current_soc is not None and record.get("soc_pct") is not None:
            soc_values.append(abs(float(record["soc_pct"]) - current_soc))
        if current_power is not None and record.get("battery_discharge_dc_kw") is not None:
            simulated_power = float(record.get("battery_discharge_dc_kw", 0)) - float(record.get("battery_charge_dc_kw", 0))
            power_errors.append(abs(simulated_power - current_power))
        if index == len(simulation) - 1:
            continue
        duration_hours = (simulation[index + 1]["time"] - timestamp).total_seconds() / 3600
        simulated_charge += float(record.get("battery_charge_dc_kw", 0)) * duration_hours
        simulated_discharge += float(record.get("battery_discharge_dc_kw", 0)) * duration_hours

    for index, record in enumerate(power[:-1]):
        duration_hours = (power[index + 1]["time"] - record["time"]).total_seconds() / 3600
        if record["value"] < 0:
            measured_charge += abs(record["value"]) * duration_hours
        else:
            measured_discharge += record["value"] * duration_hours

    if not soc_values or not power_errors:
        return SimulationComparisonReport(
            "indeterminate", "no_aligned_samples", 0, None, None, None, None, None, None
        )
    return SimulationComparisonReport(
        "available",
        "aligned_diagnostic_comparison",
        len(soc_values),
        sum(soc_values) / len(soc_values),
        sum(power_errors) / len(power_errors),
        measured_charge,
        simulated_charge,
        measured_discharge,
        simulated_discharge,
    )


def _normalise(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    for record in records:
        timestamp = record.get("time")
        value = record.get("value")
        if not isinstance(timestamp, datetime) or isinstance(value, bool):
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            continue
        if timestamp.tzinfo is None or not math.isfinite(numeric_value):
            continue
        result.append({"time": timestamp, "value": numeric_value})
    return sorted(result, key=lambda item: item["time"])


def _indeterminate(
    reason: str,
    samples: int = 0,
    charge_energy: float = 0.0,
    discharge_energy: float = 0.0,
    soc_charge_energy: float = 0.0,
    soc_discharge_energy: float = 0.0,
) -> BatteryValidationReport:
    return BatteryValidationReport(
        status="indeterminate",
        reason=reason,
        sample_count=samples,
        charge_energy_kwh=charge_energy,
        discharge_energy_kwh=discharge_energy,
        soc_charge_energy_kwh=soc_charge_energy,
        soc_discharge_energy_kwh=soc_discharge_energy,
        charge_efficiency=None,
        discharge_efficiency=None,
    )
