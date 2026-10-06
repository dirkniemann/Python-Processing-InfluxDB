import logging
import math
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
    calculate_daily_battery_efficiency,
    calculate_daily_simulation_quality,
    calculate_timeseries_simulation_errors,
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
        self.quality_valid_points = 0
        self.quality_skipped_points = 0
        self.efficiency_charge_days = 0
        self.efficiency_discharge_days = 0
        self.efficiency_charge_energy_kwh = 0.0
        self.efficiency_discharge_energy_kwh = 0.0

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
                # The handler already filters by version in InfluxDB. Its
                # normalized records intentionally contain no tag columns.
                stored_records = self._stored_daily_records(scenario_name, pv_mode)
                stored_by_day = {
                    self._record_day(record): record
                    for record in stored_records
                    if self._record_day(record) is not None
                }
                last_stored_day = self._last_stored_day(
                    scenario_name,
                    pv_mode,
                    stored_records,
                )
                start_day = (
                    last_stored_day + timedelta(days=1)
                    if last_stored_day is not None
                    else self.first_data_day
                )
                while start_day <= available_end:
                    stored = stored_by_day.get(start_day)
                    if stored is not None and stored.get("stored_energy_end_kwh") is not None:
                        start_day += timedelta(days=1)
                        continue
                    break

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
            analysis_inputs_attempted = False
            analysis_inputs = None
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
                    simulated_soc_records, simulated_daily, simulated_interval_records = self._process_day(
                        combo["name"],
                        combo["pv_mode"],
                        combo["definition"],
                        combo["state"],
                        input_day,
                    )
                    if (
                        (self.configuration.daily_quality_enabled or self.configuration.efficiency_enabled)
                        and combo["name"] == "current_battery"
                        and combo["pv_mode"] == "without_old_pv"
                    ):
                        if not analysis_inputs_attempted:
                            analysis_inputs_attempted = True
                            try:
                                analysis_inputs = self._load_daily_analysis_inputs(day)
                            except Exception:
                                logger.warning(
                                    "Could not load real daily analysis inputs for %s; "
                                    "current-battery simulation remains complete",
                                    day,
                                    exc_info=True,
                                )
                        if analysis_inputs is not None:
                            try:
                                start = local_to_utc(datetime.combine(day, time.min))
                                stop = local_to_utc(
                                    datetime.combine(day + timedelta(days=1), time.min)
                                )
                                if self.configuration.daily_quality_enabled:
                                    quality = calculate_daily_simulation_quality(
                                        simulated_soc_records=simulated_soc_records,
                                        measured_soc_records=analysis_inputs["battery_soc"],
                                        measured_grid_power_records=analysis_inputs["grid_power"],
                                        simulated_grid_import_kwh=simulated_daily.get(
                                            "grid_import_kwh", 0.0
                                        ),
                                        simulated_grid_export_kwh=simulated_daily.get(
                                            "grid_export_kwh", 0.0
                                        ),
                                        start=start,
                                        stop=stop,
                                    )
                                    if quality is None:
                                        self.quality_skipped_points += 3
                                        logger.warning(
                                            "Skipping daily quality for current_battery/%s on %s; "
                                            "real and simulated values do not cover the full day",
                                            combo["pv_mode"],
                                            day,
                                        )
                                    else:
                                        self._record_quality_stats(quality)
                                        timestamp = local_to_utc(
                                            datetime.combine(
                                                day, time(hour=23, minute=59, second=59)
                                            )
                                        )
                                        self._write_daily_quality(
                                            combo["pv_mode"], timestamp, quality
                                        )
                                    errors = calculate_timeseries_simulation_errors(
                                        simulated_records=simulated_interval_records,
                                        measured_soc_records=analysis_inputs["battery_soc"],
                                        measured_grid_power_records=analysis_inputs["grid_power"],
                                    )
                                    self._write_timeseries_errors(combo["pv_mode"], errors)
                                if self.configuration.efficiency_enabled:
                                    self._process_daily_efficiency(
                                        day,
                                        analysis_inputs["battery_power"],
                                        analysis_inputs["battery_soc"],
                                    )
                            except Exception:
                                logger.warning(
                                    "Could not calculate or write daily quality for "
                                    "current_battery/%s on %s; simulation remains complete",
                                    combo["pv_mode"],
                                    day,
                                    exc_info=True,
                                )
                finally:
                    logger.debug(
                        "Battery scenario '%s' (PV mode '%s', version '%s') day %s: "
                        "processing took %.1f s",
                        combo["name"],
                        combo["pv_mode"],
                        self.configuration.version,
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
        quality_total = self.quality_valid_points + self.quality_skipped_points
        logger.info(
            "Quality summary: valid points=%d invalid/skipped points=%d quality coverage=%.1f%%",
            self.quality_valid_points,
            self.quality_skipped_points,
            100 * self.quality_valid_points / quality_total if quality_total else 0.0,
        )
        logger.info(
            "Efficiency summary: charge valid days=%d total energy=%.3f kWh; "
            "discharge valid days=%d total energy=%.3f kWh",
            self.efficiency_charge_days,
            self.efficiency_charge_energy_kwh,
            self.efficiency_discharge_days,
            self.efficiency_discharge_energy_kwh,
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
        if self.configuration.efficiency_enabled:
            self._write_daily_efficiency_analysis(
                records["battery_power"], records["battery_soc"], start, stop
            )
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

    def _write_daily_efficiency_analysis(
        self,
        power_records: List[Dict[str, Any]],
        soc_records: List[Dict[str, Any]],
        start: datetime,
        stop: datetime,
    ) -> None:
        valid_charge_days = valid_discharge_days = 0
        total_charge_energy = total_discharge_energy = 0.0
        total_stored_energy = total_removed_energy = 0.0
        day = utc_to_local(start).date()
        last_day = utc_to_local(stop - timedelta(microseconds=1)).date()
        while day <= last_day:
            day_start = local_to_utc(datetime.combine(day, time.min))
            day_stop = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
            daily = calculate_daily_battery_efficiency(
                power_records=power_records,
                soc_records=soc_records,
                capacity_kwh=22.4,
                start=day_start,
                stop=day_stop,
            )
            timestamp = local_to_utc(
                datetime.combine(day, time(hour=23, minute=59, second=59))
            )
            points = []
            if daily.charge_efficiency is not None:
                valid_charge_days += 1
                total_charge_energy += daily.charge_energy_kwh
                total_stored_energy += daily.stored_energy_kwh
                points.append(
                    {
                        "fields": {"daily_value": daily.charge_efficiency},
                        "tags": self._tags("current_battery", "without_old_pv", "eta_charge", "ratio"),
                        "timestamp": timestamp,
                    }
                )
            if daily.discharge_efficiency is not None:
                valid_discharge_days += 1
                total_discharge_energy += daily.discharge_energy_kwh
                total_removed_energy += daily.removed_energy_kwh
                points.append(
                    {
                        "fields": {"daily_value": daily.discharge_efficiency},
                        "tags": self._tags("current_battery", "without_old_pv", "eta_discharge", "ratio"),
                        "timestamp": timestamp,
                    }
                )
            self._write_points(points)
            if daily.valid_charge_phases == 0 and daily.valid_discharge_phases == 0:
                logger.debug("Skipping battery efficiency for %s: no valid phase", day)
            day += timedelta(days=1)

        logger.info(
            "Battery efficiency summary: charge valid days=%d total energy=%.3f kWh eta_charge=%s; "
            "discharge valid days=%d total energy=%.3f kWh eta_discharge=%s",
            valid_charge_days,
            total_charge_energy,
            (total_stored_energy / total_charge_energy if total_charge_energy else None),
            valid_discharge_days,
            total_discharge_energy,
            (total_discharge_energy / total_removed_energy if total_removed_energy else None),
        )

    def _process_daily_efficiency(
        self,
        day: date,
        power_records: List[Dict[str, Any]],
        soc_records: List[Dict[str, Any]],
    ) -> None:
        start = local_to_utc(datetime.combine(day, time.min))
        stop = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        daily = calculate_daily_battery_efficiency(
            power_records=power_records,
            soc_records=soc_records,
            capacity_kwh=22.4,
            start=start,
            stop=stop,
        )
        timestamp = local_to_utc(
            datetime.combine(day, time(hour=23, minute=59, second=59))
        )
        points = []
        if daily.charge_efficiency is not None:
            self.efficiency_charge_days += 1
            self.efficiency_charge_energy_kwh += daily.charge_energy_kwh
            points.append(
                {
                    "fields": {"daily_value": daily.charge_efficiency},
                    "tags": self._tags("current_battery", "without_old_pv", "eta_charge", "ratio"),
                    "timestamp": timestamp,
                }
            )
        if daily.discharge_efficiency is not None:
            self.efficiency_discharge_days += 1
            self.efficiency_discharge_energy_kwh += daily.discharge_energy_kwh
            points.append(
                {
                    "fields": {"daily_value": daily.discharge_efficiency},
                    "tags": self._tags("current_battery", "without_old_pv", "eta_discharge", "ratio"),
                    "timestamp": timestamp,
                }
            )
        self._write_points(points)
        if not points:
            logger.debug("Skipping battery efficiency for %s: no valid phase", day)

    def _write_points(self, points: List[Dict[str, Any]]) -> None:
        if not points:
            return
        write_many = getattr(self.influx_handler, "write_fields_datapoints", None)
        if write_many is not None:
            write_many(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                datapoints=points,
            )
            return
        for point in points:
            self.influx_handler.write_fields_datapoint(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                fields=point["fields"],
                tags=point["tags"],
                timestamp=point["timestamp"],
            )

    def _required_sources(self, pv_mode: str) -> set[str]:
        required = {"corrected_house_load"}
        for group_name in self.configuration.pv_modes[pv_mode]:
            required.update(self.configuration.pv_sources[group_name])
        missing = required - set(self.configuration.sources)
        if missing:
            raise ValueError(f"PV mode {pv_mode} references unknown sources: {sorted(missing)}")
        return required

    def _load_daily_analysis_inputs(
        self, day: date
    ) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """Read real analysis sources once after current_battery ran this day."""
        sources = {}
        if self.configuration.daily_quality_enabled or self.configuration.efficiency_enabled:
            soc_source = self.configuration.validation_sources.get("battery_soc")
            if soc_source is None:
                logger.warning("Daily analysis has no configured battery SOC source")
                return None
            sources["battery_soc"] = soc_source
        if self.configuration.daily_quality_enabled:
            grid_source = self.configuration.quality_grid_power_source
            if grid_source is None:
                logger.warning("Daily quality has no configured grid-power source")
                return None
            sources["grid_power"] = grid_source
        if self.configuration.efficiency_enabled:
            power_source = self.configuration.validation_sources.get("battery_power")
            if power_source is None:
                logger.warning("Battery efficiency has no configured battery-power source")
                return None
            sources["battery_power"] = power_source

        start = local_to_utc(datetime.combine(day, time.min))
        stop = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        query_stop = stop + timedelta(minutes=15) if self.configuration.efficiency_enabled else stop
        records = {
            name: self._load_quality_source_day(source, day, start, stop, query_stop)
            for name, source in sources.items()
        }
        missing_sources = [
            name for name, source_records in records.items() if source_records is None
        ]
        if missing_sources:
            logger.warning(
                "Skipping daily analysis for %s; no valid local-midnight state "
                "for source(s): %s",
                day,
                ", ".join(missing_sources),
            )
            return None
        return records

    def _load_quality_source_day(
        self,
        source,
        day: date,
        start: datetime,
        stop: datetime,
        query_stop: Optional[datetime] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        query_stop = query_stop or stop
        history_start = LOCAL_TZ.localize(datetime(1970, 1, 1))
        previous = self.influx_handler.get_latest_datapoint_by_time(
            start_time=history_start,
            stop_time=start,
            bucket=source.bucket,
            entity_id=source.entity_id,
            field=source.field,
            measurement=source.measurement,
            version=source.version,
        )
        raw_records = self.influx_handler.get_data(
            start_time=start,
            stop_time=query_stop,
            bucket=source.bucket,
            entity_id=source.entity_id,
            field=source.field,
            measurement=source.measurement,
            version=source.version,
        )

        records_by_time: Dict[datetime, float] = {}
        invalid_sample_count = 0
        for record in raw_records:
            timestamp = record.get("time")
            value = self._quality_numeric_value(record.get("value"))
            if (
                not isinstance(timestamp, datetime)
                or timestamp.tzinfo is None
                or not start <= timestamp < query_stop
                or value is None
                or (source.name == "battery_soc" and not 0 <= value <= 100)
            ):
                invalid_sample_count += 1
                logger.warning(
                    "Ignoring invalid daily quality sample for %s: %s on %s",
                    day,
                    source.name,
                    timestamp,
                )
                continue
            records_by_time[timestamp] = value

        if invalid_sample_count:
            logger.warning(
                "Ignored %d invalid daily quality sample(s) for %s on %s",
                invalid_sample_count,
                source.name,
                day,
            )

        previous_value = (
            self._quality_numeric_value(previous.get("value"))
            if previous is not None
            else None
        )
        start_value = records_by_time.get(start, previous_value)
        if start_value is None and records_by_time:
            start_value = next(iter(records_by_time.values()))
            logger.warning(
                "Bootstrapping daily quality source %s on %s from first valid "
                "in-day sample at %s; no prior valid state was available",
                source.name,
                day,
                min(records_by_time),
            )
        if start_value is None or (
            source.name == "battery_soc" and not 0 <= start_value <= 100
        ):
            logger.warning(
                "Daily quality source %s has no valid value at local midnight on %s "
                "(bucket=%s, entity_id=%s, measurement=%r, field=%s, "
                "previous_sample=%s, valid_in_day_samples=%d)",
                source.name,
                day,
                source.bucket,
                source.entity_id,
                source.measurement,
                source.field,
                previous,
                len(records_by_time),
            )
            return None
        records_by_time[start] = start_value
        return [
            {"time": timestamp, "value": value}
            for timestamp, value in sorted(records_by_time.items())
        ]

    @staticmethod
    def _quality_numeric_value(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None
        return numeric_value if math.isfinite(numeric_value) else None

    def _write_daily_quality(self, pv_mode, timestamp, quality) -> None:
        points = []
        for entity, unit, quality_value, signed_value in (
            ("soc_pct", "%", quality.soc_mae_pct, quality.soc_signed_error_pct),
            ("grid_import", "kWh", quality.grid_import_quality_kwh, quality.grid_import_signed_error_kwh),
            ("grid_export", "kWh", quality.grid_export_quality_kwh, quality.grid_export_signed_error_kwh),
        ):
            if quality_value is None or signed_value is None:
                continue
            points.append(
                {
                    "fields": {"quality": quality_value, "signed_error": signed_value},
                    "tags": self._tags("current_battery", pv_mode, entity, unit),
                    "timestamp": timestamp,
                }
            )
        self._write_points(points)

    def _record_quality_stats(self, quality) -> None:
        values = (
            quality.soc_mae_pct,
            quality.grid_import_quality_kwh,
            quality.grid_export_quality_kwh,
        )
        self.quality_valid_points += sum(value is not None for value in values)
        self.quality_skipped_points += sum(value is None for value in values)

    def _write_timeseries_errors(self, pv_mode, errors) -> None:
        points = []
        for error in errors:
            timestamp = error["time"]
            points.extend(
                [
                    {
                        "fields": {"error": error["soc_pct"]},
                        "tags": self._tags("current_battery", pv_mode, "soc_pct", "%"),
                        "timestamp": timestamp,
                    },
                    {
                        "fields": {"error": error["grid_import_w"]},
                        "tags": self._tags("current_battery", pv_mode, "grid_import", "W"),
                        "timestamp": timestamp,
                    },
                    {
                        "fields": {"error": error["grid_export_w"]},
                        "tags": self._tags("current_battery", pv_mode, "grid_export", "W"),
                        "timestamp": timestamp,
                    },
                ]
            )
        if not points:
            return
        write_many = getattr(self.influx_handler, "write_fields_datapoints", None)
        if write_many is not None:
            write_many(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                datapoints=points,
            )
            return
        for point in points:
            self.influx_handler.write_fields_datapoint(
                bucket=self.output_bucket,
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                fields=point["fields"],
                tags=point["tags"],
                timestamp=point["timestamp"],
            )

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
                    if previous_value is not None and (
                        previous_value >= 0 or source.allow_negative
                    ):
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
                if (
                    event_time is None
                    or value is None
                    or (value < 0 and not source.allow_negative)
                ):
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

    def _last_stored_day(
        self,
        scenario_name: str,
        pv_mode: str,
        stored_records: List[Dict[str, Any]],
    ) -> Optional[date]:
        """Return the last complete output day for one scenario/PV combination."""
        method = getattr(self.influx_handler, "get_last_data_day", None)
        if method is not None:
            last_day = method(
                bucket=self.output_bucket,
                version=self.configuration.version,
                scenario=scenario_name,
                pv_mode=pv_mode,
                entity_id="stored_energy",
                measurement=BATTERY_SCENARIO_MEASUREMENT,
                field="end",
            )
            if last_day is not None:
                return last_day
        return self._last_complete_day(stored_records)

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
    ) -> Tuple[List[Dict[str, Any]], Dict[str, float], List[Dict[str, Any]]]:
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
            charge_efficiency=self.configuration.setup.charge_efficiency,
            discharge_efficiency=self.configuration.setup.discharge_efficiency,
        )
        start_energy = state.stored_energy_kwh
        daily = defaultdict(float)
        simulated_soc_records: List[Dict[str, Any]] = []
        simulated_interval_records: List[Dict[str, Any]] = []
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
            load_source = self.configuration.sources["corrected_house_load"]
            load_kw = self._require_value(
                values,
                "corrected_house_load",
                allow_negative=load_source.allow_negative,
            )
            pv_kw = self._pv_for_mode(values, pv_mode)
            if load_kw < 0:
                if not load_source.negative_as_pv:
                    raise ValueError(
                        "Negative corrected house load requires negative_as_pv=true"
                    )
                pv_kw += -load_kw
                load_kw = 0.0
                logger.debug(
                    "Treating negative corrected house load at %s as additional PV: %.6f kW",
                    timestamp,
                    -values["corrected_house_load"],
                )
            result = engine.simulate_interval(
                state=state,
                timestamp=timestamp,
                duration_s=duration_s,
                house_load_kw=load_kw,
                pv_generation_kw=pv_kw,
            )
            interval_points.extend(self._write_timeseries(scenario_name, pv_mode, result))
            self._accumulate_daily(daily, result)
            simulated_soc_records.append({"time": timestamp, "value": result.soc_pct})
            simulated_interval_records.append(
                {
                    "time": timestamp,
                    "soc_pct": result.soc_pct,
                    "grid_import_w": result.grid_import_kw * 1000,
                    "grid_export_w": result.grid_export_kw * 1000,
                }
            )

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
        return simulated_soc_records, dict(daily), simulated_interval_records

    def _pv_for_mode(self, values: Dict[str, float], pv_mode: str) -> float:
        total = 0.0
        for group_name in self.configuration.pv_modes[pv_mode]:
            for source_name in self.configuration.pv_sources[group_name]:
                total += self._require_value(values, source_name)
        return total

    @staticmethod
    def _require_value(
        values: Dict[str, float], source_name: str, allow_negative: bool = False
    ) -> float:
        if source_name not in values:
            raise ValueError(f"Missing held value for scenario source '{source_name}'")
        value = values[source_name]
        if value < 0 and not allow_negative:
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
            "house_load": (result.house_load_kw * 1000, "W"),
            "pv_generation": (result.pv_generation_kw * 1000, "W"),
            "pv_to_load": (result.pv_to_load_kw * 1000, "W"),
            "pv_to_battery": (result.pv_to_battery_kw * 1000, "W"),
            "battery_to_load": (result.battery_to_load_kw * 1000, "W"),
            "grid_import": (result.grid_import_kw * 1000, "W"),
            "grid_export": (result.grid_export_kw * 1000, "W"),
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
