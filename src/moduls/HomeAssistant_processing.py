import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from moduls.influxdb_handler import InfluxDBHandler
from moduls.influxdb_handler import LOCAL_TZ
logger = logging.getLogger(__name__)

def get_days_to_process(last_data_day: datetime) -> List[datetime]:
    """
    Get list of days that need to be processed.
    Each day is properly localized to handle DST changes.
    """
    days_to_process = []
    current_day = last_data_day + timedelta(days=1)
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    while current_day.date() <= yesterday:

        if current_day.tzinfo is not None:
            naive_day = current_day.replace(tzinfo=None)
        else:
            naive_day = current_day
        
        localized_day = LOCAL_TZ.localize(naive_day)
        days_to_process.append(localized_day)        
        current_day += timedelta(days=1)
        
    return days_to_process
class EntityProcessor:
    """Base class for entity processing strategies."""
    
    def __init__(
        self,
        influx_handler: InfluxDBHandler,
        input_bucket: str,
        output_bucket: str,
        version: str,
        entities: List[str],
        first_data_day: datetime,
        output_measurement: Optional[str] = None,
        output_entity_id: Optional[str] = None
    ):
        self.influx_handler = influx_handler
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.version = version
        self.entities = entities
        self.first_data_day = first_data_day
        self.output_measurement = output_measurement
        self.output_entity_id = output_entity_id
        
    def process(self) -> None:
        """Process entities according to the strategy."""
        raise NotImplementedError("Subclasses must implement process()")

class WaermepumpeStatistikProcessor(EntityProcessor):
    """Processor for heat pump statistics entities."""
    
    def process(self) -> None:
        """

        """
        logger.info(f"Processing {len(self.entities)} heat pump statistic entities (version: {self.version})")
        
        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version
        )

        if not last_data_day:
            logger.debug(f"No existing data for {self.output_entity_id} in output bucket, starting from first data day")
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)
        
        if not days_to_process:
            logger.debug(f"No days to process for {self.output_entity_id}")
            return
        
        logger.debug(f"Processing {len(days_to_process)} days for {self.output_entity_id}")
        
        for day in days_to_process:
            self._process_day(day)

    def _process_day(self, day: datetime) -> None:
        """

        """
        logger.debug(f"Processing day {day.date()} for heat pump statistics")
        grid_active_power = self.influx_handler.get_data(day, self.input_bucket, self.entities[2])

        sum_pv = {entity: {"values": 0.0} for entity in self.entities[:2]}
        sum_grid_import = {entity: {"values": 0.0} for entity in self.entities[:2]}
        if day == datetime(2025, 3, 18, tzinfo=LOCAL_TZ):
            logger.debug("Processing day with DST change: March 18, 2025")
        for pump_entity in self.entities[:2]:
            pump_data = self.influx_handler.get_data(day, self.input_bucket, pump_entity)

            for idx, record in enumerate(pump_data):
                if idx == 0:
                    start_value = 0
                    start_time = day.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    start_value= pump_data[idx - 1].get("value")
                    start_time = pump_data[idx - 1].get("time")

                stop_value = record.get("value")
                stop_time = record.get("time")
                kWh_diff = stop_value - start_value
                import_kwh = 0.0

                for grid_idx, grid_record in enumerate(grid_active_power):
                    grid_start_time = (
                        day.replace(hour=0, minute=0, second=0, microsecond=0)
                        if grid_idx == 0
                        else grid_active_power[grid_idx - 1].get("time")
                    )
                    grid_stop_time = grid_record.get("time")

                    if grid_start_time >= stop_time:
                        break
                    if grid_stop_time <= start_time:
                        continue

                    duration_hours = (grid_stop_time - grid_start_time).total_seconds() / 3600.0
                    if duration_hours <= 0:
                        continue

                    power_w = grid_record.get("value", 0)
                    energy_kwh = (power_w * duration_hours) / 1000.0

                    if power_w >= 0:
                        import_kwh += energy_kwh
                if import_kwh == 0:
                    waermepumpe_pv = kWh_diff
                    waermepumpe_grid = 0.0
                elif import_kwh > 0:
                    waermepumpe_pv = max(0, kWh_diff - import_kwh)
                    waermepumpe_grid = kWh_diff - waermepumpe_pv
                else:
                    logger.warning(f"Unexpected negative import_kwh value: {import_kwh} for {pump_entity} on {day.date()}")

                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=pump_entity,
                    version=self.version,
                    field="pv_contribution",
                    unit="kWh",
                    value=waermepumpe_pv,
                    timestamp=stop_time
                )
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    measurement=self.output_measurement,
                    entity_id=pump_entity,
                    version=self.version,
                    field="grid_import",
                    unit="kWh",
                    value=waermepumpe_grid,
                    timestamp=stop_time
                )

                sum_pv[pump_entity]["values"] += waermepumpe_pv
                sum_grid_import[pump_entity]["values"] += waermepumpe_grid
            
            day = day.replace(hour=23, minute=59, second=59, microsecond=0)

            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=pump_entity,
                version=self.version,
                field="daily_pv",
                unit="kWh",
                value=float(sum_pv[pump_entity]["values"]),
                timestamp=day
            )
            self.influx_handler.write_datapoint(
                bucket=self.output_bucket,
                measurement=self.output_measurement,
                entity_id=pump_entity,
                version=self.version,
                field="daily_grid_import",
                unit="kWh",
                value=sum_grid_import[pump_entity]["values"],
                timestamp=day
            )
        sum_pv_total = float(sum(sum_pv[pump_entity]["values"] for pump_entity in sum_pv))
        sum_grid_import_total = float(sum(sum_grid_import[pump_entity]["values"] for pump_entity in sum_grid_import))
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_sum_pv",
            unit="kWh",
            value=sum_pv_total,
            timestamp=day
        )
        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            measurement=self.output_measurement,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_sum_grid_import",
            unit="kWh",
            value=sum_grid_import_total,
            timestamp=day
        )


class DailyAggregateProcessor(EntityProcessor):
    """Processor for daily aggregate entities."""
            
    def process(self) -> None:
        """
        Process daily aggregate entities by calculating daily sums and writing to output bucket.
        """
        logger.info(f"Processing {len(self.entities)} daily aggregate entities (version: {self.version})")

        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.entities[0],
            version=self.version,
            measurement=self.output_measurement
        )

        if not last_data_day:
            logger.debug(f"No existing data for {self.entities[0]} in output bucket, starting from first data day")
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)
        
        if not days_to_process:
            logger.debug(f"No days to process for the daily aggregate entities")
            return
        
        for day in days_to_process:
            self._process_day(day)
    
    def _process_day(self, day: datetime) -> None:
        """Process a single day for an entity."""
        sum_value = 0.0

        for entity_id in self.entities:
            day_data = self.influx_handler.get_last_datapoint(
                start_time=day,
                bucket=self.input_bucket,
                entity_id=entity_id,
                field="value"
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
                    timestamp=day,
                    measurement=self.output_measurement
                )
                logger.debug(f"Processed daily aggregate for {entity_id} on {day.date()}: {day_data}")
            else:
                logger.warning(f"No data found for {entity_id} on {day.date()}, skipping")

        self.influx_handler.write_datapoint(
            bucket=self.output_bucket,
            entity_id=self.output_entity_id,
            version=self.version,
            field="daily_sum",
            unit="kWh",
            value=sum_value,
            timestamp=day,
            measurement=self.output_measurement
        )


class HomeAssistantProcessor:
    """
    Process Home Assistant data from InfluxDB.
    Loads data from input bucket, processes it, and writes to output bucket.
    """
    
    def __init__(
        self, 
        influx_handler: InfluxDBHandler,
        processing_config: Dict[str, Any],
        first_data_day: datetime
    ):
        """
        Initialize the Home Assistant data processor.
        
        Args:
            influx_handler: Connected InfluxDB handler instance
            processing_config: Processing configuration containing input_bucket and output_bucket
            first_data_day: The first day with data in the input bucket, used for processing logic
            
        Raises:
            ValueError: If required configuration keys are missing
        """
        self.influx_handler = influx_handler
        
        # Validate configuration
        required_keys = ["input_bucket", "output_bucket", "entities_to_process"]
        missing_keys = [key for key in required_keys if key not in processing_config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
        
        self.input_bucket = processing_config["input_bucket"]
        self.output_bucket = processing_config["output_bucket"]
        self.first_data_day = first_data_day
        
        # Initialize processors for different entity types
        self.processors: List[EntityProcessor] = []
        entities_to_process = processing_config.get("entities_to_process", {})
        
        if not isinstance(entities_to_process, dict):
            raise ValueError("'entities_to_process' must be a dictionary")
        
        # Initialize daily aggregate processor if configured
        if "daily_aggregate" in entities_to_process:
            self._init_daily_aggregate_processor(entities_to_process["daily_aggregate"])
        
        if "Waermepumpe_statistik" in entities_to_process:
            self._init_waermepumpe_statistik_processor(entities_to_process["Waermepumpe_statistik"])

        logger.debug(
            f"HomeAssistant processor initialized - Input: {self.input_bucket}, "
            f"Output: {self.output_bucket}, Processors: {len(self.processors)}"
        )
    
    def _init_daily_aggregate_processor(self, config: Dict[str, Any]) -> None:
        """Initialize the daily aggregate processor from config."""
        if not isinstance(config, dict):
            raise ValueError("'daily_aggregate' config must be a dictionary")
        
        version = config.get("version")
        if not version:
            raise ValueError("'daily_aggregate' config must include 'version'")
        
        entities = config.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'daily_aggregate.entities' must be a list")
        
        # Filter valid entities
        valid_entities = [
            entity for entity in entities 
            if isinstance(entity, str) and entity.strip()
        ]
        
        output_measurement = config.get("output_measurement")
        if output_measurement and not isinstance(output_measurement, str):
            raise ValueError("'daily_aggregate.output_measurement' must be a string if provided")
        
        output_entity_id = config.get("output_entity_id")
        if not output_entity_id or not isinstance(output_entity_id, str):
            raise ValueError("'daily_aggregate' config must include a valid 'output_entity_id'")

        if valid_entities:
            processor = DailyAggregateProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day,
                output_measurement=output_measurement,
                output_entity_id=output_entity_id
            )
            self.processors.append(processor)
            logger.debug(f"Initialized DailyAggregateProcessor with {len(valid_entities)} entities (version: {version})")
        else:
            logger.warning("No valid entities found for daily_aggregate processor")

    def _init_waermepumpe_statistik_processor(self, config: Dict[str, Any]) -> None:
        """Initialize the Waermepumpe_statistik processor from config."""
        if not isinstance(config, dict):
            raise ValueError("'Waermepumpe_statistik' config must be a dictionary")
        
        version = config.get("version")
        if not version:
            raise ValueError("'Waermepumpe_statistik' config must include 'version'")
        
        entities = config.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'Waermepumpe_statistik.entities' must be a list")
        
        # Filter valid entities
        valid_entities = [
            entity for entity in entities 
            if isinstance(entity, str) and entity.strip()
        ]

        output_measurement = config.get("output_measurement")
        if not output_measurement or not isinstance(output_measurement, str):
            raise ValueError("'Waermepumpe_statistik' config must include a valid 'output_measurement'")
        
        output_entity_id = config.get("output_entity_id")
        if not output_entity_id or not isinstance(output_entity_id, str):
            raise ValueError("'Waermepumpe_statistik' config must include a valid 'output_entity_id'")

        if valid_entities:
            processor = WaermepumpeStatistikProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day,
                output_measurement=output_measurement,
                output_entity_id=output_entity_id
            )
            self.processors.append(processor)
            logger.debug(f"Initialized WaermepumpeStatistikProcessor with {len(valid_entities)} entities (version: {version})")
        else:
            logger.warning("No valid entities found for Waermepumpe_statistik processor")

    def process_data(self) -> None:
        """
        Main method to process data from input bucket and write to output bucket.
        Executes all configured processors.
        """
        logger.info("Starting data processing...")
        
        for processor in self.processors:
            try:
                processor.process()
            except Exception as e:
                logger.error(f"Error in processor {processor.__class__.__name__}: {e}", exc_info=True)
        
        logger.info("Data processing completed.")