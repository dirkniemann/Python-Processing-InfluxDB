import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List

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
    ):
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

    def process(self) -> int:
        last_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="value",
            measurement=self.output_measurement,
        )
        if last_day is None:
            last_day = self.first_data_day - timedelta(days=1)

        days = get_days_to_process(last_day)
        for day in days:
            self._process_day(day)
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
        if day_start not in events and set(values) == source_roles:
            self._write_value(day_start, values)

        for event_time in sorted(events):
            values.update(events[event_time])
            if set(values) == source_roles:
                self._write_value(event_time, values)

        if set(values) != source_roles:
            missing = sorted(source_roles - set(values))
            raise ValueError(
                f"Missing source state for corrected house consumption on {day}: {missing}"
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
