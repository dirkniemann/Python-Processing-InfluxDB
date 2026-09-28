import logging
import time as monotonic_time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from moduls.influxdb_handler import (
    BATTERY_SCENARIO_MEASUREMENT,
    LOCAL_TZ,
    local_to_utc,
    utc_to_local,
)
from moduls.szenarios.battery_engine import BatteryScenarioEngine, BatteryState, IntervalResult
from moduls.szenarios.battery_validation import (
    BatteryValidationReport,
    analyze_real_battery,
    compare_simulation_to_real,
)
from moduls.szenarios.scenario_config import ScenarioConfiguration, ScenarioDefinitionWithSetup

logger = logging.getLogger(__name__)


@dataclass
class InputDay:
    day: date
    events: Dict[datetime, Dict[str, float]]
    initial_values: Dict[str, float]
    complete: bool
    reason: str
    source_reasons: Dict[str, str]


class BatteryScenarioRunner:
    """Incremental, restartable runner for all configured battery scenarios."""

    def __init__(
        self,
        influx_handler,
        scenario_config: ScenarioConfiguration,
        first_data_day: date,
    ):
        self.influx_handler = influx_handler
        self.configuration = scenario_config
        self.first_data_day = first_data_day
        self.output_bucket = scenario_config.buckets["output_bucket"]
        self.last_complete_date: Optional[date] = None

    def process(self, last_day: Optional[date] = None) -> int:
        if not self.configuration.definitions:
            logger.info("No enabled battery scenarios; skipping scenario processing")
            return 0
        available_end = last_day or (datetime.now(LOCAL_TZ).date() - timedelta(days=1))
        if self.first_data_day > available_end:
            return 0

        logger.info(
            "Bootstrapping first input day %s: using each source's first valid in-day "
            "sample as its starting value from midnight where no earlier state exists",
            self.first_data_day,
        )
        combinations = []
        for scenario_name, definition in self.configuration.definitions.items():
            for pv_mode in self.configuration.pv_modes:
                required_sources = self._required_sources(pv_mode)
                stored_records = [
                    record
                    for record in self._stored_daily_records(scenario_name, pv_mode)
                    if record.get("version") == self.configuration.version
                ]
                stored_by_day = {
                    self._record_day(record): record
                    for record in stored_records
                    if self._record_day(record) is not None
                }
                start_day = self.first_data_day
                while start_day <= available_end:
                    stored = stored_by_day.get(start_day)
                    if stored is None or stored.get("stored_energy_end_kwh") is None:
                        break
                    start_day += timedelta(days=1)

                if start_day > available_end:
                    logger.info(
                        "Battery scenario '%s' (PV mode '%s') is up to date; "
                        "0 day(s) to process for version '%s'",
                        scenario_name,
                        pv_mode,
                        self.configuration.version,
                    )
                    state = None
                else:
                    state = self._state_before_day(stored_records, start_day, definition)
                    logger.info(
                        "Battery scenario '%s' (PV mode '%s', version '%s'): pending %d day(s) "
                        "from %s through %s",
                        scenario_name,
                        pv_mode,
                        self.configuration.version,
                        (available_end - start_day).days + 1,
                        start_day,
                        available_end,
                    )
                combinations.append(
                    {
                        "name": scenario_name,
                        "pv_mode": pv_mode,
                        "definition": definition,
                        "required_sources": required_sources,
                        "stored_records": stored_records,
                        "start_day": start_day,
                        "state": state,
                        "processed": 0,
                        "stopped": False,
                    }
                )

        first_pending = [
            combo["start_day"] for combo in combinations if combo["start_day"] <= available_end
        ]
        if not first_pending:
            self.last_complete_date = min(
                (
                    day
                    for combo in combinations
                    if (day := self._last_complete_day(combo["stored_records"])) is not None
                ),
                default=None,
            )
            return 0

        first_day = min(first_pending)
        total_days = (available_end - first_day).days + 1
        pending_combinations = len(first_pending)
        logger.info(
            "Battery scenarios: processing up to %d day(s) sequentially across %d pending "
            "scenario/PV combination(s); output bucket '%s'",
            total_days,
            pending_combinations,
            self.output_bucket,
        )
        carried_values: Dict[str, float] = {}
        blocked_sources: set[str] = set()

        for day_index in range(total_days):
            day = first_day + timedelta(days=day_index)
            active = [
                combo
                for combo in combinations
                if not combo["stopped"] and combo["start_day"] <= day <= available_end
            ]
            active = [
                combo
                for combo in active
                if not (combo["required_sources"] & blocked_sources)
            ]
            if not active:
                continue
            if day_index == 0 or (day_index + 1) % 25 == 0 or day_index + 1 == total_days:
                logger.info(
                    "Battery scenarios: processing day %d/%d (%s), %d combination(s) active",
                    day_index + 1,
                    total_days,
                    day,
                    len(active),
                )

            sources_for_day = set().union(*(combo["required_sources"] for combo in active))
            input_started = monotonic_time.perf_counter()
            raw_day = self._load_day_inputs(day, sources_for_day, carried_values)
            logger.debug(
                "Battery inputs for day %s loaded once for %d source(s) in %.1f s",
                day,
                len(sources_for_day),
                monotonic_time.perf_counter() - input_started,
            )
            blocked_sources.update(raw_day.source_reasons)
            self._carry_day_values(raw_day, carried_values)

            for combo in active:
                failed_sources = combo["required_sources"] & blocked_sources
                if failed_sources:
                    combo["stopped"] = True
                    logger.warning(
                        "Stopping battery scenario '%s' (PV mode '%s') at %s; "
                        "invalid input source(s): %s",
                        combo["name"],
                        combo["pv_mode"],
                        day,
                        ", ".join(sorted(failed_sources)),
                    )
                    continue
                input_day = self._select_input_sources(raw_day, combo["required_sources"])
                scenario_started = monotonic_time.perf_counter()
                try:
                    self._process_day(
                        combo["name"],
                        combo["pv_mode"],
                        combo["definition"],
                        combo["state"],
                        input_day,
                    )
                finally:
                    logger.debug(
                        "Battery scenario '%s' (PV mode '%s', version '%s') day %s: "
                        "processing took %.1f s",
                        combo["name"],
                        combo["pv_mode"],
                        self.configuration.setup.version,
                        day,
                        monotonic_time.perf_counter() - scenario_started,
                    )
                combo["processed"] += 1
                combo["stored_records"].append(
                    {
                        "time": local_to_utc(
                            datetime.combine(day, time(hour=23, minute=59, second=59))
                        ),
                        "stored_energy_end_kwh": combo["state"].stored_energy_kwh,
                    }
                )

        processed_days = 0
        complete_dates: List[date] = []
        for combo in combinations:
            processed_days = max(processed_days, combo["processed"])
            logger.info(
                "Battery scenario '%s' (PV mode '%s'): processed %d day(s)",
                combo["name"],
                combo["pv_mode"],
                combo["processed"],
            )
            latest_complete = self._last_complete_day(combo["stored_records"])
            if latest_complete is not None:
                complete_dates.append(latest_complete)
        self.last_complete_date = min(complete_dates) if complete_dates else None
        return processed_days

    def validate_real_battery(self, last_day: Optional[date] = None) -> Optional[BatteryValidationReport]:
        """Compare measured battery power/SOC and never feed the result into V1."""
        if not self.configuration.definitions:
            logger.info("No enabled battery scenarios; skipping real battery validation")
            return None
        sources = self.configuration.validation_sources
        if set(sources) != {"battery_power", "battery_soc"}:
            return None
        end_day = last_day or (datetime.now(LOCAL_TZ).date() - timedelta(days=1))
        start = local_to_utc(datetime.combine(self.first_data_day, time.min))
        stop = local_to_utc(datetime.combine(end_day + timedelta(days=1), time.min))
        records: Dict[str, List[Dict[str, Any]]] = {}
        for name, source in sources.items():
            raw_records = self.influx_handler.get_data(
                start_time=start,
                stop_time=stop,
                bucket=source.bucket,
                entity_id=source.entity_id,
                field=source.field,
                measurement=source.measurement,
                version=source.version,
            )
            records[name] = list(raw_records)
        power_records = [
            {"time": item["time"], "value": float(item["value"]) / 1000}
            for item in records["battery_power"]
        ]
        report = analyze_real_battery(
            power_records=power_records,
            soc_records=records["battery_soc"],
            capacity_kwh=self.configuration.setup.base_capacity_kwh,
        )
        logger.info("Real battery validation: %s", report)
        timeseries_method = getattr(self.influx_handler, "get_scenario_timeseries_records", None)
        if timeseries_method is not None:
            simulation_records = timeseries_method(
                bucket=self.output_bucket,
                scenario="current_battery",
                pv_mode="without_old_pv",
                version=self.configuration.version,
            )
            if simulation_records:
                comparison = compare_simulation_to_real(
                    simulated_records=simulation_records,
                    power_records=power_records,
                    soc_records=records["battery_soc"],
                )
                logger.info("Current battery simulation comparison: %s", comparison)
        return report

    def _required_sources(self, pv_mode: str) -> set[str]:
        required = {"corrected_house_load"}
        for group_name in self.configuration.pv_modes[pv_mode]:
            required.update(self.configuration.pv_sources[group_name])
        missing = required - set(self.configuration.sources)
        if missing:
            raise ValueError(f"PV mode {pv_mode} references unknown sources: {sorted(missing)}")
        return required

    def _load_day_inputs(
        self,
        day: date,
        required_sources: set[str],
        carried_values: Optional[Dict[str, float]] = None,
    ) -> InputDay:
        day_start = local_to_utc(datetime.combine(day, time.min))
        day_end = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        events: Dict[datetime, Dict[str, float]] = defaultdict(dict)
        initial_values: Dict[str, float] = {}
        source_reasons: Dict[str, str] = {}
        history_start = LOCAL_TZ.localize(datetime(1970, 1, 1))

        for name, source in self.configuration.sources.items():
            if name not in required_sources:
                continue
            reasons: List[str] = []
            if carried_values is not None and name in carried_values:
                initial_values[name] = carried_values[name]
            else:
                previous = self.influx_handler.get_latest_datapoint_by_time(
                    start_time=history_start,
                    stop_time=day_start,
                    bucket=source.bucket,
                    entity_id=source.entity_id,
                    field=source.field,
                    measurement=source.measurement,
                    version=source.version,
                )
                if previous is not None:
                    previous_value = self._numeric_value(previous.get("value"), name)
                    if previous_value is not None and previous_value >= 0:
                        initial_values[name] = previous_value
                    elif previous_value is not None:
                        reasons.append(f"invalid_previous_value:{name}")

            try:
                records = self.influx_handler.get_data(
                    start_time=day_start,
                    stop_time=day_end,
                    bucket=source.bucket,
                    entity_id=source.entity_id,
                    field=source.field,
                    measurement=source.measurement,
                    version=source.version,
                )
            except Exception as exc:
                message = (
                    f"InfluxDB-Abfrage für Szenario-Eingabe '{name}' am {day} "
                    f"ist fehlgeschlagen (Bucket '{source.bucket}', "
                    f"Entity '{source.entity_id}'). Der Lauf wird abgebrochen: {exc}"
                )
                logger.error(message)
                raise RuntimeError(message) from exc
            source_event_count = 0
            first_in_day_value: Optional[Tuple[datetime, float]] = None
            for record in records:
                event_time = record.get("time")
                value = self._numeric_value(record.get("value"), name)
                if event_time is None or value is None or value < 0:
                    reasons.append(f"invalid_value:{name}")
                    continue
                if day_start <= event_time < day_end:
                    events[event_time][name] = value
                    source_event_count += 1
                    if first_in_day_value is None or event_time < first_in_day_value[0]:
                        first_in_day_value = (event_time, value)

            if name not in initial_values and source_event_count == 0:
                reasons.append(f"missing_initial_state:{name}")
            elif name not in initial_values:
                if day == self.first_data_day and first_in_day_value is not None:
                    initial_values[name] = first_in_day_value[1]
                else:
                    reasons.append(f"missing_pre_window_state:{name}")
            elif source_event_count == 0:
                if not source.change_only:
                    reasons.append(f"no_event_non_change_only:{name}")
            if reasons:
                source_reasons[name] = ";".join(dict.fromkeys(reasons))

        input_day = InputDay(
            day=day,
            events=dict(events),
            initial_values=initial_values,
            complete=not source_reasons,
            reason=";".join(source_reasons.values()) or "complete",
            source_reasons=source_reasons,
        )
        return input_day

    @staticmethod
    def _select_input_sources(input_day: InputDay, required_sources: set[str]) -> InputDay:
        source_reasons = {
            source: reason
            for source, reason in input_day.source_reasons.items()
            if source in required_sources
        }
        return InputDay(
            day=input_day.day,
            events={
                timestamp: {
                    source: value
                    for source, value in values.items()
                    if source in required_sources
                }
                for timestamp, values in input_day.events.items()
            },
            initial_values={
                source: value
                for source, value in input_day.initial_values.items()
                if source in required_sources
            },
            complete=not source_reasons,
            reason=";".join(source_reasons.values()) or "complete",
            source_reasons=source_reasons,
        )

    @staticmethod
    def _carry_day_values(input_day: InputDay, carried_values: Dict[str, float]) -> None:
        for source in input_day.source_reasons:
            carried_values.pop(source, None)
        for source, value in input_day.initial_values.items():
            if source not in input_day.source_reasons:
                carried_values[source] = value
        for event_time in sorted(input_day.events):
            for source, value in input_day.events[event_time].items():
                if source not in input_day.source_reasons:
                    carried_values[source] = value

    @staticmethod
    def _numeric_value(value: Any, source_name: str) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, str):
            if value.lower() in {"unknown", "unavailable", "none"}:
                return None
            try:
                value = float(value)
            except ValueError:
                return None
        if not isinstance(value, (int, float)):
            return None
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return float(value) / 1000

    def _stored_daily_records(self, scenario_name: str, pv_mode: str) -> List[Dict[str, Any]]:
        method = getattr(self.influx_handler, "get_scenario_daily_records", None)
        if method is None:
            return []
        return method(
            bucket=self.output_bucket,
            scenario=scenario_name,
            pv_mode=pv_mode,
            version=self.configuration.version,
        )

    @staticmethod
    def _record_day(record: Dict[str, Any]) -> Optional[date]:
        timestamp = record.get("time")
        if isinstance(timestamp, datetime):
            return utc_to_local(timestamp).date()
        return None

    @staticmethod
    def _last_complete_day(records: List[Dict[str, Any]]) -> Optional[date]:
        complete_days = [
            BatteryScenarioRunner._record_day(record)
            for record in records
            if record.get("stored_energy_end_kwh") is not None
        ]
        valid_days = [day for day in complete_days if day is not None]
        return max(valid_days) if valid_days else None

    def _state_before_day(
        self,
        records: List[Dict[str, Any]],
        day: date,
        definition: ScenarioDefinitionWithSetup,
    ) -> BatteryState:
        previous = [
            record
            for record in records
            if self._record_day(record) is not None and self._record_day(record) < day
            and record.get("stored_energy_end_kwh") is not None
        ]
        if previous:
            latest = max(previous, key=lambda record: self._record_day(record))
            return BatteryState(float(latest["stored_energy_end_kwh"]))
        return BatteryState(
            definition.capacity_kwh * self.configuration.setup.initial_soc_pct / 100
        )

    def _process_day(
        self,
        scenario_name: str,
        pv_mode: str,
        definition: ScenarioDefinitionWithSetup,
        state: BatteryState,
        input_day: InputDay,
    ) -> None:
        day_start = local_to_utc(datetime.combine(input_day.day, time.min))
        day_stop = local_to_utc(datetime.combine(input_day.day + timedelta(days=1), time.min))
        daily_timestamp = local_to_utc(
            datetime.combine(input_day.day, time(hour=23, minute=59, second=59))
        )
        required_sources = {"corrected_house_load"}
        for group_name in self.configuration.pv_modes[pv_mode]:
            required_sources.update(self.configuration.pv_sources[group_name])
        missing_initial_values = required_sources - set(input_day.initial_values)
        if missing_initial_values:
            raise RuntimeError(
                f"Cannot simulate complete day {input_day.day}: no valid starting value for "
                f"{', '.join(sorted(missing_initial_values))}"
            )

        timestamps = sorted({day_start, day_stop, *input_day.events})
        values = dict(input_day.initial_values)
        engine = BatteryScenarioEngine(
            definition=definition,
            min_soc_pct=self.configuration.setup.min_soc_pct,
            max_soc_pct=self.configuration.setup.max_soc_pct,
        )
        start_energy = state.stored_energy_kwh
        daily = defaultdict(float)
        interval_points = []
        for index, timestamp in enumerate(timestamps[:-1]):
            values.update(input_day.events.get(timestamp, {}))
            next_timestamp = timestamps[index + 1]
            duration_s = (next_timestamp - timestamp).total_seconds()
            missing_values = required_sources - set(values)
            if missing_values:
                raise RuntimeError(
                    f"Cannot simulate interval at {timestamp}: missing valid values for "
                    f"{', '.join(sorted(missing_values))}"
                )
            load_kw = self._require_value(values, "corrected_house_load")
            pv_kw = self._pv_for_mode(values, pv_mode)
            result = engine.simulate_interval(
                state=state,
                timestamp=timestamp,
                duration_s=duration_s,
                house_load_kw=load_kw,
                pv_generation_kw=pv_kw,
            )
            interval_points.extend(self._write_timeseries(scenario_name, pv_mode, result))
            self._accumulate_daily(daily, result)

        write_many = getattr(self.influx_handler, "write_fields_datapoints", None)
        if write_many is not None:
            write_many(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                datapoints=interval_points,
            )
        else:
            for point in interval_points:
                self.influx_handler.write_fields_datapoint(
                    bucket=self.output_bucket,
                    measurement=BATTERY_SCENARIO_MEASUREMENT,
                    fields=point["fields"],
                    tags=point["tags"],
                    timestamp=point["timestamp"],
                )
        self._write_daily(
            scenario_name,
            pv_mode,
            input_day,
            daily_timestamp,
            start_energy,
            state,
            daily,
        )

    def _pv_for_mode(self, values: Dict[str, float], pv_mode: str) -> float:
        total = 0.0
        for group_name in self.configuration.pv_modes[pv_mode]:
            for source_name in self.configuration.pv_sources[group_name]:
                total += self._require_value(values, source_name)
        return total

    @staticmethod
    def _require_value(values: Dict[str, float], source_name: str) -> float:
        if source_name not in values:
            raise ValueError(f"Missing held value for scenario source '{source_name}'")
        value = values[source_name]
        if value < 0:
            raise ValueError(f"Negative value for scenario source '{source_name}'")
        return value

    def _tags(
        self,
        scenario_name: str,
        pv_mode: str,
        entity_id: str,
        unit: str,
    ) -> Dict[str, str]:
        return {
            "entity_id": entity_id,
            "scenario": scenario_name,
            "pv_mode": pv_mode,
            "version": self.configuration.version,
            "unit": unit,
        }

    def _write_timeseries(
        self,
        scenario_name: str,
        pv_mode: str,
        result: IntervalResult,
    ) -> List[Dict[str, Any]]:
        actual_values = {
            "soc_pct": (result.soc_pct, "%"),
            "stored_energy": (result.stored_energy_kwh, "kWh"),
            "house_load": (result.house_load_kw, "kW"),
            "pv_generation": (result.pv_generation_kw, "kW"),
            "pv_to_load": (result.pv_to_load_kw, "kW"),
            "pv_to_battery": (result.pv_to_battery_kw, "kW"),
            "battery_to_load": (result.battery_to_load_kw, "kW"),
            "grid_import": (result.grid_import_kw, "kW"),
            "grid_export": (result.grid_export_kw, "kW"),
        }
        return [
            {
                "fields": {"actual": value},
                "tags": self._tags(scenario_name, pv_mode, entity_id, unit),
                "timestamp": result.timestamp,
            }
            for entity_id, (value, unit) in actual_values.items()
        ]

    @staticmethod
    def _accumulate_daily(daily: Dict[str, float], result: IntervalResult) -> None:
        hours = result.duration_s / 3600
        for field_name in (
            "pv_generation_kw",
            "pv_to_load_kw",
            "pv_to_battery_kw",
            "battery_to_load_kw",
            "grid_import_kw",
            "grid_export_kw",
        ):
            daily[field_name.replace("_kw", "_kwh")] += getattr(result, field_name) * hours

    def _write_daily(
        self,
        scenario_name: str,
        pv_mode: str,
        input_day: InputDay,
        timestamp: datetime,
        start_energy: float,
        state: BatteryState,
        daily: Dict[str, float],
    ) -> None:
        definition = self.configuration.definitions[scenario_name]
        daily_points = [
            {
                "fields": {"daily_sum": value},
                "tags": self._tags(
                    scenario_name,
                    pv_mode,
                    field_name.removesuffix("_kwh"),
                    "kWh",
                ),
                "timestamp": timestamp,
            }
            for field_name, value in daily.items()
        ]
        daily_points.extend(
            [
                {
                    "fields": {
                        "start": start_energy / definition.capacity_kwh * 100,
                        "end": state.stored_energy_kwh / definition.capacity_kwh * 100,
                    },
                    "tags": self._tags(scenario_name, pv_mode, "soc_pct", "%"),
                    "timestamp": timestamp,
                },
                {
                    "fields": {
                        "start": start_energy,
                        "end": state.stored_energy_kwh,
                    },
                    "tags": self._tags(scenario_name, pv_mode, "stored_energy", "kWh"),
                    "timestamp": timestamp,
                },
            ]
        )
        write_many = getattr(self.influx_handler, "write_fields_datapoints", None)
        if write_many is not None:
            write_many(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                datapoints=daily_points,
            )
        else:
            for point in daily_points:
                self.influx_handler.write_fields_datapoint(
                    bucket=self.output_bucket,
                    measurement=BATTERY_SCENARIO_MEASUREMENT,
                    fields=point["fields"],
                    tags=point["tags"],
                    timestamp=point["timestamp"],
                )
