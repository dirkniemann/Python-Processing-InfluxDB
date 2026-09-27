import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from moduls.influxdb_handler import LOCAL_TZ, local_to_utc, utc_to_local
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
    fingerprint: str
    complete: bool
    uncertain: bool
    reason: str


class BatteryScenarioRunner:
    """Incremental, restartable runner for all configured battery scenarios."""

    model_version = "v2"

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
        self._input_cache: Dict[Tuple[date, Tuple[str, ...]], InputDay] = {}

    def process(self, last_day: Optional[date] = None) -> int:
        if not self.configuration.definitions:
            logger.info("No enabled battery scenarios; skipping scenario processing")
            return 0
        available_end = last_day or (datetime.now(LOCAL_TZ).date() - timedelta(days=1))
        if self.first_data_day > available_end:
            return 0

        processed_days = 0
        for scenario_name, definition in self.configuration.definitions.items():
            for pv_mode in self.configuration.pv_modes:
                required_sources = self._required_sources(pv_mode)
                input_days = self._collect_input_days(available_end, required_sources)
                if not input_days:
                    continue
                processed_days = max(
                    processed_days,
                    self._process_combination_plan(
                        scenario_name, pv_mode, definition, input_days
                    ),
                )
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
            )
            if simulation_records:
                latest_run = simulation_records[-1].get("run_version")
                simulation_records = [
                    record for record in simulation_records
                    if record.get("run_version") == latest_run
                ]
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

    def _collect_input_days(self, available_end: date, required_sources: set[str]) -> Dict[date, InputDay]:
        input_days: Dict[date, InputDay] = {}
        day = self.first_data_day
        while day <= available_end:
            input_day = self._load_day_inputs(day, required_sources)
            input_days[day] = input_day
            if not input_day.complete:
                logger.warning(
                    "Stopping scenario input horizon at %s: %s",
                    day,
                    input_day.reason,
                )
                break
            day += timedelta(days=1)
        return input_days

    def _load_day_inputs(self, day: date, required_sources: set[str]) -> InputDay:
        cache_key = (day, tuple(sorted(required_sources)))
        cached = self._input_cache.get(cache_key)
        if cached is not None:
            return cached

        day_start = local_to_utc(datetime.combine(day, time.min))
        day_end = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        events: Dict[datetime, Dict[str, float]] = defaultdict(dict)
        initial_values: Dict[str, float] = {}
        fingerprint_events: List[Dict[str, Any]] = []
        reasons: List[str] = []
        complete = True
        uncertain = False
        history_start = LOCAL_TZ.localize(datetime(1970, 1, 1))

        for name, source in self.configuration.sources.items():
            if name not in required_sources:
                continue
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
                if previous_value is not None:
                    initial_values[name] = previous_value

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
                records = []
                complete = False
                reasons.append(f"query_error:{name}:{type(exc).__name__}")

            source_event_count = 0
            for record in records:
                event_time = record.get("time")
                value = self._numeric_value(record.get("value"), name)
                if event_time is None or value is None:
                    complete = False
                    reasons.append(f"invalid_value:{name}")
                    continue
                if day_start <= event_time < day_end:
                    events[event_time][name] = value
                    fingerprint_events.append(
                        {"source": name, "time": event_time.isoformat(), "value": value}
                    )
                    source_event_count += 1

            if name not in initial_values and source_event_count == 0:
                complete = False
                reasons.append(f"missing_initial_state:{name}")
            elif name not in initial_values:
                uncertain = True
                reasons.append(f"missing_pre_window_state:{name}")
            elif source_event_count == 0:
                if source.change_only:
                    uncertain = True
                    reasons.append(f"held_change_only:{name}")
                else:
                    complete = False
                    reasons.append(f"no_event_non_change_only:{name}")

        if complete and not initial_values and not events:
            complete = False
            reasons.append("no_input_events")
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "initial": initial_values,
                    "events": sorted(fingerprint_events, key=lambda item: (item["time"], item["source"])),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:20]
        input_day = InputDay(
            day=day,
            events=dict(events),
            initial_values=initial_values,
            fingerprint=fingerprint,
            complete=complete,
            uncertain=uncertain,
            reason=";".join(reasons) or "complete",
        )
        self._input_cache[cache_key] = input_day
        return input_day

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

    def _process_combination_plan(
        self,
        scenario_name: str,
        pv_mode: str,
        definition: ScenarioDefinitionWithSetup,
        input_days: Dict[date, InputDay],
    ) -> int:
        stored_records = self._stored_daily_records(scenario_name, pv_mode)
        current_records = [
            record
            for record in stored_records
            if record.get("config_hash") == self.configuration.config_hash
            and record.get("model_version") == self.model_version
        ]
        run_reason = "initial"
        run_version: Optional[str] = None
        start_day = self.first_data_day

        mismatch_day = self._find_input_mismatch(current_records, input_days)
        if mismatch_day is not None:
            run_reason = "historical_input_change"
            run_version = self._make_run_version(run_reason, mismatch_day, input_days[mismatch_day])
        elif current_records:
            run_version = str(current_records[-1].get("run_version"))
            last_complete = self._last_complete_day(current_records)
            if last_complete is not None:
                start_day = last_complete + timedelta(days=1)
        else:
            old_records = [record for record in stored_records if record]
            if old_records:
                run_reason = "model_or_config_change"
            run_version = self._make_run_version(run_reason, self.first_data_day, input_days[self.first_data_day])

        if run_version is None:
            run_version = self._make_run_version(run_reason, start_day, input_days[start_day])

        if mismatch_day is not None or run_reason == "model_or_config_change":
            start_day = self.first_data_day

        if start_day not in input_days:
            return 0
        state = self._state_before_day(current_records, start_day, definition)
        processed = 0
        for day in sorted(input_days):
            if day < start_day:
                continue
            input_day = input_days[day]
            if not input_day.complete:
                self._write_incomplete_daily(
                    scenario_name, pv_mode, run_version, run_reason, input_day, state, definition
                )
                break
            self._process_day(
                scenario_name,
                pv_mode,
                run_version,
                run_reason,
                definition,
                state,
                input_day,
            )
            processed += 1
        return processed

    def _stored_daily_records(self, scenario_name: str, pv_mode: str) -> List[Dict[str, Any]]:
        method = getattr(self.influx_handler, "get_scenario_daily_records", None)
        if method is None:
            return []
        return method(
            bucket=self.output_bucket,
            scenario=scenario_name,
            pv_mode=pv_mode,
        )

    def _find_input_mismatch(
        self,
        records: List[Dict[str, Any]],
        input_days: Dict[date, InputDay],
    ) -> Optional[date]:
        by_day = {self._record_day(record): record for record in records if self._record_day(record)}
        for day in sorted(input_days):
            stored = by_day.get(day)
            if stored is None:
                continue
            if stored.get("input_fingerprint") != input_days[day].fingerprint:
                return day
        return None

    @staticmethod
    def _record_day(record: Dict[str, Any]) -> Optional[date]:
        local_day = record.get("local_day")
        if isinstance(local_day, str):
            try:
                return date.fromisoformat(local_day)
            except ValueError:
                pass
        timestamp = record.get("time")
        if isinstance(timestamp, datetime):
            return utc_to_local(timestamp).date()
        return None

    @staticmethod
    def _last_complete_day(records: List[Dict[str, Any]]) -> Optional[date]:
        complete_days = [
            record
            for record in records
            if record.get("is_complete") in (True, 1, 1.0)
        ]
        if not complete_days:
            return None
        return max(
            record["local_day"]
            for record in complete_days
            if isinstance(record.get("local_day"), str)
        ) and date.fromisoformat(
            max(record["local_day"] for record in complete_days if isinstance(record.get("local_day"), str))
        )

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
            and record.get("is_complete") in (True, 1, 1.0)
        ]
        if previous:
            latest = max(previous, key=lambda record: self._record_day(record))
            return BatteryState(float(latest["stored_energy_end_kwh"]))
        return BatteryState(
            definition.capacity_kwh * self.configuration.setup.initial_soc_pct / 100
        )

    def _make_run_version(self, reason: str, day: date, input_day: InputDay) -> str:
        return (
            f"{self.model_version}-{self.configuration.config_hash}-"
            f"{reason}-{day.isoformat()}-{input_day.fingerprint[:8]}"
        )

    def _process_day(
        self,
        scenario_name: str,
        pv_mode: str,
        run_version: str,
        run_reason: str,
        definition: ScenarioDefinitionWithSetup,
        state: BatteryState,
        input_day: InputDay,
    ) -> None:
        day_start = local_to_utc(datetime.combine(input_day.day, time.min))
        day_end = local_to_utc(datetime.combine(input_day.day + timedelta(days=1), time.min))
        timestamps = sorted({day_start, day_end, *input_day.events})
        values = dict(input_day.initial_values)
        engine = BatteryScenarioEngine(
            definition=definition,
            min_soc_pct=self.configuration.setup.min_soc_pct,
            max_soc_pct=self.configuration.setup.max_soc_pct,
        )
        start_energy = state.stored_energy_kwh
        daily = defaultdict(float)
        interval_uncertain = input_day.uncertain
        unknown_duration_s = 0.0

        for index, timestamp in enumerate(timestamps[:-1]):
            values.update(input_day.events.get(timestamp, {}))
            next_timestamp = timestamps[index + 1]
            duration_s = (next_timestamp - timestamp).total_seconds()
            if "corrected_house_load" not in values or any(
                source_name not in values
                for group_name in self.configuration.pv_modes[pv_mode]
                for source_name in self.configuration.pv_sources[group_name]
            ):
                unknown_duration_s += duration_s
                continue
            load_kw = self._require_value(values, "corrected_house_load")
            pv_kw = self._pv_for_mode(values, pv_mode)
            result = engine.simulate_interval(
                state=state,
                timestamp=timestamp,
                duration_s=duration_s,
                house_load_kw=load_kw,
                pv_generation_kw=pv_kw,
            )
            self._write_timeseries(
                scenario_name, pv_mode, run_version, run_reason, result, interval_uncertain
            )
            self._accumulate_daily(daily, result)

        self._write_daily(
            scenario_name,
            pv_mode,
            run_version,
            run_reason,
            input_day,
            day_end,
            start_energy,
            state,
            daily,
            interval_uncertain,
            unknown_duration_s,
        )

    def _write_incomplete_daily(
        self,
        scenario_name: str,
        pv_mode: str,
        run_version: str,
        run_reason: str,
        input_day: InputDay,
        state: BatteryState,
        definition: ScenarioDefinitionWithSetup,
    ) -> None:
        soc = state.stored_energy_kwh / definition.capacity_kwh * 100
        self.influx_handler.write_fields_datapoint(
            bucket=self.output_bucket,
            measurement="battery_scenario_daily",
            fields={
                "soc_start_pct": soc,
                "soc_end_pct": soc,
                "stored_energy_start_kwh": state.stored_energy_kwh,
                "stored_energy_end_kwh": state.stored_energy_kwh,
                "valid_duration_s": 0.0,
                "is_complete": False,
                "quality_uncertain": True,
                "quality_reason_code": input_day.reason,
                "input_fingerprint": input_day.fingerprint,
                "local_day": input_day.day.isoformat(),
                "config_hash": self.configuration.config_hash,
            },
            tags=self._tags(scenario_name, pv_mode, run_version, run_reason),
            timestamp=local_to_utc(datetime.combine(input_day.day + timedelta(days=1), time.min)),
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
        self, scenario_name: str, pv_mode: str, run_version: str, run_reason: str
    ) -> Dict[str, str]:
        return {
            "scenario": scenario_name,
            "pv_mode": pv_mode,
            "run_version": run_version,
            "run_reason": run_reason,
            "model_version": self.model_version,
        }

    def _write_timeseries(
        self,
        scenario_name: str,
        pv_mode: str,
        run_version: str,
        run_reason: str,
        result: IntervalResult,
        uncertain: bool,
    ) -> None:
        fields = {
            "soc_pct": result.soc_pct,
            "stored_energy_kwh": result.stored_energy_kwh,
            "house_load_kw": result.house_load_kw,
            "pv_generation_kw": result.pv_generation_kw,
            "pv_to_load_kw": result.pv_to_load_kw,
            "pv_to_battery_kw": result.pv_to_battery_kw,
            "battery_to_load_kw": result.battery_to_load_kw,
            "battery_charge_dc_kw": result.battery_charge_dc_kw,
            "battery_discharge_dc_kw": result.battery_discharge_dc_kw,
            "grid_import_kw": result.grid_import_kw,
            "grid_export_kw": result.grid_export_kw,
            "pv_export_kw": result.pv_export_kw,
            "quality_valid": result.quality_valid,
            "quality_uncertain": uncertain,
        }
        self.influx_handler.write_fields_datapoint(
            bucket=self.output_bucket,
            measurement="battery_scenario_timeseries",
            fields=fields,
            tags=self._tags(scenario_name, pv_mode, run_version, run_reason),
            timestamp=result.timestamp,
        )

    @staticmethod
    def _accumulate_daily(daily: Dict[str, float], result: IntervalResult) -> None:
        hours = result.duration_s / 3600
        for field_name in (
            "pv_generation_kw",
            "pv_to_load_kw",
            "pv_to_battery_kw",
            "pv_export_kw",
            "battery_to_load_kw",
            "battery_charge_dc_kw",
            "battery_discharge_dc_kw",
            "grid_import_kw",
            "grid_export_kw",
        ):
            daily[field_name.replace("_kw", "_kwh")] += getattr(result, field_name) * hours
        daily["valid_duration_s"] += result.duration_s

    def _write_daily(
        self,
        scenario_name: str,
        pv_mode: str,
        run_version: str,
        run_reason: str,
        input_day: InputDay,
        timestamp: datetime,
        start_energy: float,
        state: BatteryState,
        daily: Dict[str, float],
        uncertain: bool,
        unknown_duration_s: float,
    ) -> None:
        definition = self.configuration.definitions[scenario_name]
        fields = dict(daily)
        fields.update(
            {
                "soc_start_pct": start_energy / definition.capacity_kwh * 100,
                "soc_end_pct": state.stored_energy_kwh / definition.capacity_kwh * 100,
                "stored_energy_start_kwh": start_energy,
                "stored_energy_end_kwh": state.stored_energy_kwh,
                "is_complete": unknown_duration_s == 0,
                "quality_uncertain": uncertain,
                "quality_reason_code": (
                    input_day.reason
                    if unknown_duration_s == 0
                    else f"{input_day.reason};unknown_duration_s={unknown_duration_s}"
                ),
                "input_fingerprint": input_day.fingerprint,
                "local_day": input_day.day.isoformat(),
                "config_hash": self.configuration.config_hash,
            }
        )
        self.influx_handler.write_fields_datapoint(
            bucket=self.output_bucket,
            measurement="battery_scenario_daily",
            fields=fields,
            tags=self._tags(scenario_name, pv_mode, run_version, run_reason),
            timestamp=timestamp,
        )
