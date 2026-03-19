import logging
from datetime import datetime, timedelta, time
from moduls.processing.HomeAssistant_processor import EntityProcessor, get_days_to_process
from moduls.influxdb_handler import local_to_utc

logger = logging.getLogger(__name__)


class DailyAggregateProcessor(EntityProcessor):
    """Processor for daily aggregate entities."""

    def process(self) -> None:
        """Process daily aggregate entities by calculating daily sums and writing to output bucket."""
        logger.info(
            f"Processing {len(self.entities)} daily aggregate entities (version: {self.version})"
        )

        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            field="daily_sum",
            version=self.version,
            measurement=self.output_measurement,
        )

        if not last_data_day:
            logger.debug(
                f"No existing data for {self.output_entity_id} in output bucket, starting from first data day"
            )
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)

        if not days_to_process:
            logger.warning("No days to process for the daily aggregate entities")
            return

        logger.info(f"Processing {len(days_to_process)} days for {self.output_entity_id}")

        last_version = self.influx_handler.get_last_version(
            bucket=self.output_bucket,
            entity_id=self.entities[0],
            field="value",
            measurement="fix_waermepumpe_stromverbrauch",
        )

        for day in days_to_process:
            self._process_day(day, last_version)

    def _process_day(self, day: datetime, last_version: str) -> None:
        """Process a single day for an entity."""
        sum_value = 0.0
        day_start_time = local_to_utc(datetime.combine(day, time(hour=0, minute=0, second=0, microsecond=0)))
        day_end_time = local_to_utc(datetime.combine(day, time(hour=23, minute=59, second=59, microsecond=999999)))

        for entity_id in self.entities:
            day_data = self.influx_handler.get_last_datapoint(
                start_time=day_start_time,
                stop_time=day_end_time,
                bucket=self.output_bucket,
                measurement="fix_waermepumpe_stromverbrauch",
                entity_id=entity_id,
                field="value",
                version=last_version
            )

            if day_data is not None:
                sum_value += day_data["value"]
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    entity_id=entity_id,
                    version=self.version,
                    field="daily_sum",
                    unit="kWh",
                    value=day_data["value"],
                    timestamp=day_end_time,
                    measurement=self.output_measurement,
                )
                logger.debug(
                    f"Processed daily aggregate for {entity_id} on {day}: {day_data}"
                )
            else:
                logger.error(f"No data found for {entity_id} on {day}")
                raise ValueError(f"No data found for {entity_id} on {day}")

        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_sum",
            unit="kWh",
            value=sum_value,
            timestamp=day_end_time,
            measurement=self.output_measurement,
        )
