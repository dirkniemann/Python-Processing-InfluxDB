import logging
from datetime import datetime, timedelta, time
from moduls.processing.HomeAssistant_processor import EntityProcessor, get_days_to_process
from moduls.influxdb_handler import local_to_utc

logger = logging.getLogger(__name__)


class WaermepumpeStatistikProcessor(EntityProcessor):
    """Processor for heat pump statistics entities."""

    def process(self) -> None:
        """Process daily heat pump statistics for configured entities.

        Args:
            None
        Returns:
            None. Writes PV contribution and grid import per entity plus daily totals.
        """
        logger.info(
            f"Processing {len(self.entities)} heat pump statistic entities (version: {self.version})"
        )

        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_pv",
            measurement=self.output_measurement,
        )

        if not last_data_day:
            logger.debug(
                f"No existing data for {self.output_entity_id} in output bucket, starting from first data day"
            )
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)

        if not days_to_process:
            logger.warning(f"No days to process for {self.output_entity_id}")
            return

        last_version = self.influx_handler.get_last_version(
            bucket=self.output_bucket,
            entity_id=self.entities[0],
            field="value",
            measurement="fix_waermepumpe_stromverbrauch",
        )

        if not last_version:
            raise RuntimeError(
                "No processed version found for fix_waermepumpe_stromverbrauch; cannot compute statistics"
            )

        logger.info(f"Processing {len(days_to_process)} days for {self.output_entity_id}")

        for day in days_to_process:
            self._process_day(day, last_version)

    def _process_day(self, day: datetime.date, last_version: str) -> None:
        """Process a single day's worth of heat pump data.

        Args:
            day: Calendar day to process (date without tz).
            last_version: Version tag of processed consumption data to read from.
        Returns:
            None. Writes per-interval PV/grid split, daily totals, and combined totals.
        """
        logger.debug(f"Processing day {day} for heat pump statistics")
        day_start_time = local_to_utc(datetime.combine(day, time(hour=0, minute=0, second=0, microsecond=0)))
        day_end_time = local_to_utc(datetime.combine(day, time(hour=23, minute=59, second=59, microsecond=999999)))
        grid_active_power = self.influx_handler.get_data(
            start_time=day_start_time,
            stop_time=day_end_time,
            bucket=self.input_bucket,
            entity_id=self.entities[2])

        sum_pv = {entity: {"values": 0.0} for entity in self.entities[:2]}
        sum_grid_import = {entity: {"values": 0.0} for entity in self.entities[:2]}
        for pump_entity in self.entities[:2]:
            pump_data = self.influx_handler.get_data(
                start_time=day_start_time,
                stop_time=day_end_time, 
                bucket=self.output_bucket, 
                entity_id=pump_entity, 
                field="value",
                version=last_version,
                measurement="fix_waermepumpe_stromverbrauch"
            )

            for idx, record in enumerate(pump_data):
                start_value = record.get("value")
                start_time = record.get("time")

                if idx + 1 < len(pump_data):
                    stop_value = pump_data[idx + 1].get("value")
                    stop_time = pump_data[idx + 1].get("time")
                else:
                    break

                kWh_diff = stop_value - start_value
                import_kwh = 0.0
                grid_duration_hours = 0.0
                for grid_idx, grid_record in enumerate(grid_active_power):
                    grid_start_time = grid_record.get("time")
                    if grid_idx + 1 < len(grid_active_power):
                        grid_stop_time = grid_active_power[grid_idx + 1].get("time")
                    else:
                        break

                    if grid_start_time >= stop_time:
                        break
                    if grid_stop_time <= start_time:
                        continue

                    duration_hours = (grid_stop_time - grid_start_time).total_seconds() / 3600.0
                    if duration_hours <= 0:
                        continue
                    grid_duration_hours += duration_hours
                    power_w = grid_record.get("value", 0)
                    energy_kwh = (power_w * duration_hours) / 1000.0

                    if power_w >= 0:
                        import_kwh += energy_kwh
                pump_duration_hours = (stop_time - start_time).total_seconds() / 3600.0
                import_kwh = import_kwh * (pump_duration_hours / grid_duration_hours) if grid_duration_hours > 0 else 0.0
                if import_kwh == 0:
                    waermepumpe_pv = kWh_diff
                    waermepumpe_grid = 0.0
                elif import_kwh > 0:
                    waermepumpe_pv = max(0, kWh_diff - import_kwh)
                    waermepumpe_grid = kWh_diff - waermepumpe_pv
                else:
                    logger.error(
                        f"Unexpected negative import_kwh value: {import_kwh} for {pump_entity} on {day}"
                    )
                    raise ValueError(
                        f"Unexpected negative import_kwh value: {import_kwh} for {pump_entity} on {day}"
                    )
                if waermepumpe_pv < 0 or waermepumpe_grid < 0:
                    logger.error(
                        f"Negative contribution calculated for {pump_entity} on {day}: PV={waermepumpe_pv}, Grid={waermepumpe_grid}"
                    )
                    raise ValueError(
                        f"Negative contribution calculated for {pump_entity} on {day}: PV={waermepumpe_pv}, Grid={waermepumpe_grid}"
                    )
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=pump_entity,
                    version=self.version,
                    field="pv_contribution",
                    unit="kWh",
                    value=waermepumpe_pv,
                    timestamp=stop_time,
                )
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=pump_entity,
                    version=self.version,
                    field="grid_import",
                    unit="kWh",
                    value=waermepumpe_grid,
                    timestamp=stop_time,
                )

                sum_pv[pump_entity]["values"] += waermepumpe_pv
                sum_grid_import[pump_entity]["values"] += waermepumpe_grid



            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=pump_entity,
                version=self.version,
                field="daily_pv",
                unit="kWh",
                value=float(sum_pv[pump_entity]["values"]),
                timestamp=day_end_time,
            )
            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=pump_entity,
                version=self.version,
                field="daily_grid_import",
                unit="kWh",
                value=sum_grid_import[pump_entity]["values"],
                timestamp=day_end_time,
            )
        sum_pv_total = float(sum(sum_pv[pump_entity]["values"] for pump_entity in sum_pv))
        sum_grid_import_total = float(
            sum(sum_grid_import[pump_entity]["values"] for pump_entity in sum_grid_import)
        )
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_pv",
            unit="kWh",
            value=sum_pv_total,
            timestamp=day_end_time,
        )
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_grid_import",
            unit="kWh",
            value=sum_grid_import_total,
            timestamp=day_end_time,
        )
