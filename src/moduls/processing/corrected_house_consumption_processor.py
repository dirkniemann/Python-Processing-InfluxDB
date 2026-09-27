import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict

from moduls.influxdb_handler import LOCAL_TZ, local_to_utc
from moduls.processing.HomeAssistant_processor import EntityProcessor, get_days_to_process

logger = logging.getLogger(__name__)


class CorrectedHouseConsumptionProcessor(EntityProcessor):
    """Build corrected house load from change-only power sources."""

    def __init__(
        self,
        influx_handler,
        input_bucket: str,
        output_bucket: str,
        version: str,
        sources: Dict[str, Dict[str, str]],
        first_data_day: date,
        output_measurement: str,
        output_entity_id: str,
        interval_seconds: int = 300,
    ):
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, int)
            or interval_seconds <= 0
        ):
            raise ValueError("interval_seconds must be a positive integer")
        super().__init__(
            influx_handler=influx_handler,
            input_bucket=input_bucket,
            output_bucket=output_bucket,
            version=version,
            entities=[],
            first_data_day=first_data_day,
            output_measurement=output_measurement,
            output_entity_id=output_entity_id,
        )
        self.sources = sources
        self.interval_seconds = interval_seconds

    def process(self) -> int:
        last_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="value",
            measurement=self.output_measurement,
        )
        existing_last_day = last_day
        if last_day is None:
            last_day = self.first_data_day - timedelta(days=1)

        days = get_days_to_process(last_day)
        for day in days:
            self._process_day(day)
        self.last_complete_date = days[-1] if days else existing_last_day
        return len(days)

    def _process_day(self, day: date) -> None:
        day_start = local_to_utc(datetime.combine(day, time.min))
        day_end = local_to_utc(datetime.combine(day + timedelta(days=1), time.min))
        values = self._load_start_values(day_start)
        events: Dict[datetime, Dict[str, Any]] = {}

        for role, source in self.sources.items():
            records = self.influx_handler.get_data(
                start_time=day_start,
                stop_time=day_end,
                bucket=source["bucket"],
                entity_id=source["entity_id"],
                field=source["field"],
                measurement=source["measurement"],
                version=source.get("version"),
            )
            for record in records:
                event_time = record["time"]
                if event_time < day_start or event_time >= day_end:
                    continue
                events.setdefault(event_time, {})[role] = record["value"]

        if not values and not events:
            raise ValueError(f"No input data found for corrected house consumption on {day}")

        source_roles = set(self.sources)
        ordered_events = sorted(events.items())
        event_index = 0
        bin_start = day_start
        written_intervals = 0
        skipped_initial_intervals = 0
        while bin_start < day_end:
            bin_end = min(
                bin_start + timedelta(seconds=self.interval_seconds),
                day_end,
            )

            # Apply changes exactly on the interval boundary before deciding
            # whether this interval has complete source coverage.
            while (
                event_index < len(ordered_events)
                and ordered_events[event_index][0] <= bin_start
            ):
                values.update(ordered_events[event_index][1])
                event_index += 1

            if set(values) != source_roles:
                # At the beginning of the first data day, one or both sensors
                # may not yet have a known state. Never invent a value or
                # average only part of a 5-minute interval as if it covered
                # the whole interval. Skip until the next fully covered bin.
                while (
                    event_index < len(ordered_events)
                    and ordered_events[event_index][0] < bin_end
                ):
                    values.update(ordered_events[event_index][1])
                    event_index += 1
                skipped_initial_intervals += 1
                bin_start = bin_end
                continue

            covered_seconds = (bin_end - bin_start).total_seconds()
            energy_w_seconds = {role: 0.0 for role in source_roles}
            cursor = bin_start

            while event_index < len(ordered_events):
                event_time, changes = ordered_events[event_index]
                if event_time >= bin_end:
                    break
                if event_time > cursor:
                    elapsed = (event_time - cursor).total_seconds()
                    for role in source_roles:
                        energy_w_seconds[role] += float(values[role]) * elapsed
                    cursor = event_time
                values.update(changes)
                event_index += 1

            if cursor < bin_end:
                elapsed = (bin_end - cursor).total_seconds()
                for role in source_roles:
                    energy_w_seconds[role] += float(values[role]) * elapsed

            interval_means = {
                role: energy / covered_seconds
                for role, energy in energy_w_seconds.items()
            }
            self._write_value(bin_start, interval_means)
            written_intervals += 1
            bin_start = bin_end

        if written_intervals == 0:
            missing = sorted(source_roles - set(values))
            detail = f"; missing source states: {missing}" if missing else ""
            logger.warning(
                "No complete %s-second intervals available for corrected house "
                "consumption on %s%s; skipping this day",
                self.interval_seconds,
                day,
                detail,
            )
            return
        if skipped_initial_intervals:
            logger.warning(
                "Skipped %s initial interval(s) for corrected house consumption on %s "
                "because source state was not yet known",
                skipped_initial_intervals,
                day,
            )

    def _load_start_values(self, day_start: datetime) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        history_start = LOCAL_TZ.localize(datetime(1970, 1, 1))
        for role, source in self.sources.items():
            previous = self.influx_handler.get_latest_datapoint_by_time(
                start_time=history_start,
                stop_time=day_start,
                bucket=source["bucket"],
                entity_id=source["entity_id"],
                field=source["field"],
                measurement=source["measurement"],
                version=source.get("version"),
            )
            if previous is not None:
                values[role] = previous["value"]
        return values

    def _write_value(self, timestamp: datetime, values: Dict[str, Any]) -> None:
        if set(values) != set(self.sources):
            missing = sorted(set(self.sources) - set(values))
            raise ValueError(f"Missing source state for corrected house consumption: {missing}")
        corrected_value = sum(float(values[role]) for role in self.sources)
        if corrected_value < 0:
            logger.warning(
                "Corrected house consumption below zero at %s (raw sum: %.3f W); "
                "clamping to 0 W",
                timestamp,
                corrected_value,
            )
            corrected_value = 0.0
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            value=corrected_value,
            field="value",
            version=self.version,
            unit="W",
            timestamp=timestamp,
            measurement=self.output_measurement,
        )
