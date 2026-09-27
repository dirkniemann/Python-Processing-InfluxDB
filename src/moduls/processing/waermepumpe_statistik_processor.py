import logging
from datetime import datetime, timedelta, time
from typing import Dict, List, Any, Optional, Iterable, Tuple
from moduls.processing.HomeAssistant_processor import EntityProcessor, get_days_to_process
from moduls.influxdb_handler import local_to_utc

logger = logging.getLogger(__name__)


def calculate_positive_counter_delta(previous_value: float, current_value: float) -> float:
    """Return counter energy while treating a decrease as a reset."""
    previous = float(previous_value)
    current = float(current_value)
    if previous < 0 or current < 0:
        raise ValueError("Counter values must not be negative")
    if current < previous:
        return 0.0
    return current - previous


def allocate_common_grid_budget(
    pump_energy: Dict[str, float], grid_import_kwh: float
) -> Dict[str, float]:
    """Allocate one positive grid budget proportionally across both pumps."""
    normalized_energy = {}
    for entity_id, energy in pump_energy.items():
        value = float(energy)
        if value < 0:
            raise ValueError(f"Pump energy must not be negative: {entity_id}")
        normalized_energy[entity_id] = value

    budget = max(float(grid_import_kwh), 0.0)
    total_energy = sum(normalized_energy.values())
    if total_energy == 0.0:
        return {entity_id: 0.0 for entity_id in normalized_energy}

    allocation_factor = min(1.0, budget / total_energy)
    return {
        entity_id: energy * allocation_factor
        for entity_id, energy in normalized_energy.items()
    }


class WaermepumpeStatistikProcessor(EntityProcessor):
    """Processor for heat pump statistics entities."""

    def __init__(
        self,
        *args,
        sensor_roles: Optional[Dict[str, Dict[str, str]]] = None,
        source_version: Optional[str] = None,
        compressor_post_run_minutes: float = 0.0,
        emit_daily_summary: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sensor_roles = sensor_roles or {}
        self.source_version = source_version or self.version
        self.compressor_post_run_minutes = compressor_post_run_minutes
        self.emit_daily_summary = emit_daily_summary

    def process(self) -> int:
        """Process all complete days not yet present for this calculation version."""
        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_pv",
            measurement=self.output_measurement,
        )
        if not last_data_day:
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)
        if not days_to_process:
            logger.info("No heat pump statistic days to process")
            return 0

        logger.info("Processing %d heat pump statistic days", len(days_to_process))
        for day in days_to_process:
            self._process_day(day, self.source_version)

        return len(days_to_process)

    def _sensor(self, role: str) -> Dict[str, str]:
        try:
            return self.sensor_roles[role]
        except KeyError as exc:
            raise RuntimeError(f"Missing configured sensor role: {role}") from exc

    @staticmethod
    def _sorted_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            (record for record in records if record.get("time") is not None),
            key=lambda record: record["time"],
        )

    @staticmethod
    def _interval_boundaries(
        start: datetime,
        stop: datetime,
        pump_records: Dict[str, List[Dict[str, Any]]],
    ) -> List[datetime]:
        boundaries = {start, stop}
        for records in pump_records.values():
            boundaries.update(
                record["time"] for record in records if start < record["time"] < stop
            )
        return sorted(boundaries)

    @staticmethod
    def _energy_for_interval(
        records: List[Dict[str, Any]], start: datetime, stop: datetime
    ) -> float:
        total = 0.0
        for index, record in enumerate(records[:-1]):
            record_start = record["time"]
            record_stop = records[index + 1]["time"]
            overlap_start = max(start, record_start)
            overlap_stop = min(stop, record_stop)
            duration = (record_stop - record_start).total_seconds()
            if overlap_stop <= overlap_start or duration <= 0:
                continue
            delta = calculate_positive_counter_delta(
                record.get("value", 0.0), records[index + 1].get("value", 0.0)
            )
            total += delta * (
                (overlap_stop - overlap_start).total_seconds() / duration
            )
        return total

    @staticmethod
    def _grid_import_kwh(
        records: List[Dict[str, Any]], start: datetime, stop: datetime
    ) -> float:
        total = 0.0
        for index, record in enumerate(records):
            record_start = record["time"]
            record_stop = records[index + 1]["time"] if index + 1 < len(records) else stop
            overlap_start = max(start, record_start)
            overlap_stop = min(stop, record_stop)
            if overlap_stop <= overlap_start:
                continue
            duration_hours = (overlap_stop - overlap_start).total_seconds() / 3600.0
            total += max(float(record.get("value", 0.0)), 0.0) * duration_hours / 1000.0
        return total

    @staticmethod
    def _active_seconds(
        events: List[Dict[str, Any]],
        start: datetime,
        stop: datetime,
        post_run_minutes: float = 0.0,
    ) -> float:
        state = None
        active_start = None
        active_intervals: List[Tuple[datetime, datetime]] = []
        post_run = timedelta(minutes=post_run_minutes)

        for event in events:
            if event["time"] <= start:
                state = int(event.get("value", 0))
            else:
                break
        if state == 1:
            active_start = start

        for event in events:
            event_time = event["time"]
            value = int(event.get("value", 0))
            if event_time <= start:
                state = value
                continue
            if event_time >= stop:
                break
            if state == 1 and active_start is None:
                active_start = start
            if value == 1 and state != 1:
                active_start = event_time
            elif value == 0 and state == 1 and active_start is not None:
                active_intervals.append((active_start, min(stop, event_time + post_run)))
                active_start = None
            state = value

        if state == 1 and active_start is not None:
            active_intervals.append((active_start, stop))
        elif state is None and not events:
            return 0.0

        active_seconds = 0.0
        for active_start, active_stop in active_intervals:
            overlap_start = max(start, active_start)
            overlap_stop = min(stop, active_stop)
            if overlap_stop > overlap_start:
                active_seconds += (overlap_stop - overlap_start).total_seconds()
        return active_seconds

    @staticmethod
    def _count_start_events(
        events: List[Dict[str, Any]], start: datetime, stop: datetime
    ) -> int:
        """Count new 0-to-1 transitions within the requested period."""
        state = None
        starts = 0
        for event in events:
            event_time = event["time"]
            value = int(event.get("value", 0))
            if event_time <= start:
                state = value
                continue
            if event_time >= stop:
                break
            if value == 1 and state != 1:
                starts += 1
            state = value
        return starts

    def _weighted_energy_by_interval(
        self,
        records: List[Dict[str, Any]],
        activity_events: List[Dict[str, Any]],
        boundaries: List[datetime],
    ) -> Dict[Tuple[datetime, datetime], float]:
        result = {(start, stop): 0.0 for start, stop in zip(boundaries, boundaries[1:])}
        for record, next_record in zip(records, records[1:]):
            segment_start = record["time"]
            segment_stop = next_record["time"]
            if segment_stop <= segment_start:
                continue
            intervals = []
            for interval in result:
                overlap_start = max(segment_start, interval[0])
                overlap_stop = min(segment_stop, interval[1])
                if overlap_stop > overlap_start:
                    intervals.append((interval, overlap_start, overlap_stop))
            if not intervals:
                continue

            active_weights = {
                interval: self._active_seconds(
                    activity_events,
                    overlap_start,
                    overlap_stop,
                    self.compressor_post_run_minutes,
                )
                for interval, overlap_start, overlap_stop in intervals
            }
            total_active = sum(active_weights.values())
            weights = active_weights if total_active > 0 else {
                interval: (overlap_stop - overlap_start).total_seconds()
                for interval, overlap_start, overlap_stop in intervals
            }
            total_weight = sum(weights.values())
            delta = calculate_positive_counter_delta(
                record.get("value", 0.0), next_record.get("value", 0.0)
            )
            if total_weight > 0:
                for interval, weight in weights.items():
                    result[interval] += delta * weight / total_weight
        return result

    def _process_day(self, day: datetime.date, source_version: str) -> None:
        logger.debug("Processing heat pump statistics for %s", day)
        day_start = local_to_utc(datetime.combine(day, time.min))
        day_stop = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        query_start = day_start - timedelta(days=1)

        pump_records = {}
        activity_events = {}
        for pump_role, compressor_role in (
            ("heat_pump_1_counter", "compressor_1"),
            ("heat_pump_2_counter", "compressor_2"),
        ):
            counter = self._sensor(pump_role)
            pump_records[pump_role] = self._sorted_records(
                self.influx_handler.get_data(
                    start_time=day_start,
                    stop_time=day_stop,
                    bucket=self.output_bucket,
                    entity_id=counter["entity_id"],
                    field=counter["field"],
                    version=source_version,
                    measurement="fix_waermepumpe_stromverbrauch",
                )
            )
            compressor = self._sensor(compressor_role)
            activity_events[compressor_role] = self._sorted_records(
                self.influx_handler.get_data(
                    start_time=query_start,
                    stop_time=day_stop,
                    bucket=self.input_bucket,
                    entity_id=compressor["entity_id"],
                    field=compressor["field"],
                    measurement=compressor["measurement"],
                )
            )

        grid = self._sensor("grid_power")
        grid_records = self._sorted_records(
            self.influx_handler.get_data(
                start_time=query_start,
                stop_time=day_stop,
                bucket=self.input_bucket,
                entity_id=grid["entity_id"],
                field=grid["field"],
                measurement=grid["measurement"],
            )
        )

        pump_entities = {
            role: self._sensor(role)["entity_id"]
            for role in ("heat_pump_1_counter", "heat_pump_2_counter")
        }
        boundaries = self._interval_boundaries(day_start, day_stop, pump_records)
        interval_list = list(zip(boundaries, boundaries[1:]))
        pump_energy_by_role = {
            role: self._weighted_energy_by_interval(
                pump_records[role],
                activity_events[compressor_role],
                boundaries,
            )
            for role, compressor_role in (
                ("heat_pump_1_counter", "compressor_1"),
                ("heat_pump_2_counter", "compressor_2"),
            )
        }
        daily_pv = {entity: 0.0 for entity in pump_entities.values()}
        daily_grid = {entity: 0.0 for entity in pump_entities.values()}
        active_interval_counts = {"compressor_1": 0, "compressor_2": 0}
        both_active_intervals = 0
        compressor_cycle_counts = {
            compressor_role: self._count_start_events(
                activity_events[compressor_role], day_start, day_stop
            )
            for compressor_role in ("compressor_1", "compressor_2")
        }

        for interval_start, interval_stop in interval_list:
            duration_seconds = (interval_stop - interval_start).total_seconds()
            if duration_seconds <= 0:
                continue
            active_by_role = {
                compressor_role: self._active_seconds(
                    activity_events[compressor_role],
                    interval_start,
                    interval_stop,
                    self.compressor_post_run_minutes,
                )
                for compressor_role in ("compressor_1", "compressor_2")
            }
            for compressor_role, active_seconds in active_by_role.items():
                if active_seconds > 0:
                    active_interval_counts[compressor_role] += 1
            if all(active_seconds > 0 for active_seconds in active_by_role.values()):
                both_active_intervals += 1
            pump_energy = {
                pump_entities[role]: pump_energy_by_role[role][
                    (interval_start, interval_stop)
                ]
                for role in pump_records
            }
            total_energy = sum(pump_energy.values())
            if total_energy <= 0:
                continue

            grid_import = self._grid_import_kwh(grid_records, interval_start, interval_stop)
            grid_shares = allocate_common_grid_budget(pump_energy, grid_import)
            for entity_id, energy in pump_energy.items():
                grid_share = grid_shares[entity_id]
                pv_share = energy - grid_share
                daily_grid[entity_id] += grid_share
                daily_pv[entity_id] += pv_share
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=entity_id,
                    version=self.version,
                    field="pv_contribution",
                    unit="kWh",
                    value=pv_share,
                    timestamp=interval_stop,
                )
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=entity_id,
                    version=self.version,
                    field="grid_import",
                    unit="kWh",
                    value=grid_share,
                    timestamp=interval_stop,
                )

        day_timestamp = day_stop - timedelta(microseconds=1)
        for entity_id in pump_entities.values():
            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=entity_id,
                version=self.version,
                field="daily_pv",
                unit="kWh",
                value=daily_pv[entity_id],
                timestamp=day_timestamp,
            )
            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=entity_id,
                version=self.version,
                field="daily_grid_import",
                unit="kWh",
                value=daily_grid[entity_id],
                timestamp=day_timestamp,
            )

        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_pv",
            unit="kWh",
            value=sum(daily_pv.values()),
            timestamp=day_timestamp,
        )
        if self.emit_daily_summary:
            total_pv = sum(daily_pv.values())
            total_grid = sum(daily_grid.values())
            logger.info(
                "v2 day %s summary: WP1=%.3f kWh, WP2=%.3f kWh, "
                "grid=%.3f kWh, rest_pv=%.3f kWh, "
                "counter_intervals_with_activity=(WP1:%d, WP2:%d, both:%d), "
                "compressor_starts=(WP1:%d, WP2:%d)",
                day,
                daily_pv[pump_entities["heat_pump_1_counter"]]
                + daily_grid[pump_entities["heat_pump_1_counter"]],
                daily_pv[pump_entities["heat_pump_2_counter"]]
                + daily_grid[pump_entities["heat_pump_2_counter"]],
                total_grid,
                total_pv,
                active_interval_counts["compressor_1"],
                active_interval_counts["compressor_2"],
                both_active_intervals,
                compressor_cycle_counts["compressor_1"],
                compressor_cycle_counts["compressor_2"],
            )
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_grid_import",
            unit="kWh",
            value=sum(daily_grid.values()),
            timestamp=day_timestamp,
        )
